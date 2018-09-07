//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "core/Worker.h"
#include "protocol/RStore/RStore.h"

namespace scar {

template <class Workload> class RStoreSwitcher : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using ProtocolType = RStore<DatabaseType>;

  using RWKeyType = typename WorkloadType::RWKeyType;
  using TableType = typename DatabaseType::TableType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType =
      typename ProtocolType::template MessageFactoryType<TableType>;
  using MessageHandlerType =
      typename ProtocolType::template MessageHandlerType<TableType,
                                                         TransactionType>;

  using StorageType = typename WorkloadType::StorageType;

  RStoreSwitcher(std::size_t coordinator_id, std::size_t id,
                 ContextType &context, std::atomic<bool> &stopFlag,
                 std::atomic<uint32_t> &worker_status,
                 std::atomic<uint32_t> &n_complete_workers)
      : Worker(coordinator_id, id), context(context), stopFlag(stopFlag),
        worker_status(worker_status), n_completed_workers(n_complete_workers) {

    for (auto i = 0u; i < context.coordinatorNum; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }
  }

  void coordinator_start() {

    std::size_t n_workers = context.coordinatorNum;
    std::size_t n_coordinators = context.workerNum;

    while (!stopFlag.load()) {

      // start c-phase

      n_completed_workers.store(0);
      signal_worker(RStoreWorkerStatus::C_PHASE);

      // only for debug
      std::this_thread::sleep_for(std::chrono::seconds(1));

      // wait for all workers to finish
      while (n_completed_workers.load() < n_workers) {
        // change to nop_pause()?
        std::this_thread::yield();
      }

      broadcast_stop();
      wait4_ack(RStoreWorkerStatus::C_PHASE);

      // start c-phase

      n_completed_workers.store(0);

      signal_worker(RStoreWorkerStatus::S_PHASE);

      // only for debug
      std::this_thread::sleep_for(std::chrono::seconds(1));

      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      wait4_ack(RStoreWorkerStatus::S_PHASE);
    }
  }

  void non_coordinator_start() {
    std::size_t n_workers = context.coordinatorNum;
    std::size_t n_coordinators = context.workerNum;

    for (;;) {

      RStoreWorkerStatus status = wait4_signal();
      if (status == RStoreWorkerStatus::EXIT) {
        set_worker_status(RStoreWorkerStatus::EXIT);
        break;
      }

      LOG(INFO) << "start C-Phase";

      // start c-phase

      DCHECK(status == RStoreWorkerStatus::C_PHASE);
      n_completed_workers.store(0);
      set_worker_status(RStoreWorkerStatus::C_PHASE);
      wait4_stop(1);
      set_worker_status(RStoreWorkerStatus::STOP);
      // wait for all workers to finish
      while (n_completed_workers.load() < n_workers) {
        std::this_thread::yield();
      }
      send_ack(RStoreWorkerStatus::C_PHASE);

      LOG(INFO) << "start S-Phase";

      // start s-phase

      status = wait4_signal();
      DCHECK(status == RStoreWorkerStatus::S_PHASE);
      n_completed_workers.store(0);
      set_worker_status(RStoreWorkerStatus::S_PHASE);
      // wait for all workers to finish
      while (n_completed_workers.load() < n_workers) {
        std::this_thread::yield();
      }
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      send_ack(RStoreWorkerStatus::S_PHASE);
    }
  }

  void set_worker_status(RStoreWorkerStatus status) {
    worker_status.store(static_cast<int>(status));
  }

  void signal_worker(RStoreWorkerStatus status) {

    // only the coordinator node calls this function
    DCHECK(coordinator_id == 0);
    set_worker_status(status);

    // signal to everyone
    for (auto i = 0u; i < context.coordinatorNum; i++) {
      if (i == coordinator_id)
        continue;
      MessageFactoryType::new_worker_status_message(*messages[i], status);
    }
    flush_messages();
  }

  RStoreWorkerStatus wait4_signal() {
    // only non-coordinator calls this function
    DCHECK(coordinator_id != 0);

    while (signal_in_queue.empty()) {
      std::this_thread::yield();
    }

    std::unique_ptr<Message> message(signal_in_queue.front());
    bool ok = signal_in_queue.pop();
    CHECK(ok);

    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = messagePiece.get_message_type();
    CHECK(type == static_cast<int>(RStoreMessage::SIGNAL));

    uint32_t status;
    StringPiece stringPiece = messagePiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> status;

    return static_cast<RStoreWorkerStatus>(status);
  }

  void wait4_stop(std::size_t n) {}

  void send_ack(RStoreWorkerStatus status) {}

  void wait4_ack(RStoreWorkerStatus status) {}

  void broadcast_stop() {}

  void start() override {

    if (coordinator_id == 0) {
      LOG(INFO) << "Switcher on the coordinator node started.";
      coordinator_start();
      LOG(INFO) << "Switcher on the coordinator node exits.";
    } else {
      LOG(INFO) << "Switcher on the non-coordinator node started.";
      non_coordinator_start();
      LOG(INFO) << "Switcher on the non-coordinator node exits.";
    }
  }

  void push_message(Message *message) override {

    // message will only be of type signal, C_PHASE_ACK or S_PHASE_ACK

    auto message_type =
        static_cast<RStoreMessage>(message->get_message_count());

    switch (message_type) {
    case RStoreMessage::SIGNAL:
      signal_in_queue.push(message);
      break;
    case RStoreMessage::C_PHASE_ACK:
      ack_in_queue.push(message);
      break;
    case RStoreMessage::S_PHASE_ACK:
      ack_in_queue.push(message);
      break;

    default:
      CHECK(false);
      break;
    }
  }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();
    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }

  /*
  std::size_t process_request() {

    std::size_t size = 0;

    while (!inQueue.empty()) {
      std::unique_ptr<Message> message(inQueue.front());
      bool ok = inQueue.pop();
      CHECK(ok);
      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = static_cast<RStoreMessage>(messagePiece.get_message_type());

        DCHECK(type == RStoreMessage::SIGNAL);
        signal_handler(messagePiece);
      }

      size += message->get_message_count();
    }
    return size;
  }
   */

  void signal_handler(MessagePiece messagePiece) {}

private:
  void flush_messages() {

    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = messages[i].release();

      out_queue.push(message);
      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    }
  }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

private:
  ContextType &context;
  std::atomic<bool> &stopFlag;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_completed_workers;
  RandomType random;
  LockfreeQueue<Message *> ack_in_queue, signal_in_queue, out_queue;
  std::vector<std::unique_ptr<Message>> messages;
};
} // namespace scar