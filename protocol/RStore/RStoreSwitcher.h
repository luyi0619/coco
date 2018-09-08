//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "core/Worker.h"

#include "protocol/RStore/RStoreHelper.h"
#include "protocol/RStore/RStoreMessage.h"

namespace scar {

template <class Workload> class RStoreSwitcher : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = typename WorkloadType::TransactionType;

  using TableType = typename DatabaseType::TableType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using MessageType = RStoreMessage;

  using MessageFactoryType = RStoreMessageFactory<TableType>;
  using MessageHandlerType = RStoreMessageHandler<TableType>;

  RStoreSwitcher(std::size_t coordinator_id, std::size_t id,
                 ContextType &context, std::atomic<bool> &stopFlag)
      : Worker(coordinator_id, id), context(context), stopFlag(stopFlag) {

    for (auto i = 0u; i < context.coordinatorNum; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }
  }

  void coordinator_start() {

    std::size_t n_workers = context.workerNum;
    std::size_t n_coordinators = context.coordinatorNum;

    while (!stopFlag.load()) {

      // start c-phase

      n_completed_workers.store(0);
      signal_worker(RStoreWorkerStatus::C_PHASE);
      // only for debug
      std::this_thread::sleep_for(std::chrono::seconds(1));
      wait_all_workers_finish();
      set_worker_status(RStoreWorkerStatus::STOP);
      broadcast_stop();
      wait4_ack(RStoreWorkerStatus::C_PHASE);

      // start s-phase

      n_completed_workers.store(0);
      signal_worker(RStoreWorkerStatus::S_PHASE);
      // only for debug
      std::this_thread::sleep_for(std::chrono::seconds(1));
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      // process replication
      n_completed_workers.store(0);
      set_worker_status(RStoreWorkerStatus::STOP);
      wait_all_workers_finish();
      wait4_ack(RStoreWorkerStatus::S_PHASE);
    }
  }

  void non_coordinator_start() {

    std::size_t n_workers = context.workerNum;
    std::size_t n_coordinators = context.coordinatorNum;

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
      wait_all_workers_finish();
      send_ack(RStoreWorkerStatus::C_PHASE);

      LOG(INFO) << "start S-Phase";

      // start s-phase

      status = wait4_signal();
      DCHECK(status == RStoreWorkerStatus::S_PHASE);
      n_completed_workers.store(0);
      set_worker_status(RStoreWorkerStatus::S_PHASE);
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      // process replication
      n_completed_workers.store(0);
      set_worker_status(RStoreWorkerStatus::STOP);
      wait_all_workers_finish();
      send_ack(RStoreWorkerStatus::S_PHASE);
    }
  }

  void wait_all_workers_finish() {
    std::size_t n_workers = context.workerNum;
    // wait for all workers to finish
    while (n_completed_workers.load() < n_workers) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  void set_worker_status(RStoreWorkerStatus status) {
    worker_status.store(static_cast<uint32_t>(status));
  }

  void signal_worker(RStoreWorkerStatus status) {

    // only the coordinator node calls this function
    DCHECK(coordinator_id == 0);
    DCHECK(status == RStoreWorkerStatus::C_PHASE ||
           status == RStoreWorkerStatus::S_PHASE);
    set_worker_status(status);

    // signal to everyone
    for (auto i = 0u; i < context.coordinatorNum; i++) {
      if (i == coordinator_id) {
        continue;
      }
      MessageFactoryType::new_signal_message(*messages[i], status);
    }
    flush_messages();
  }

  RStoreWorkerStatus wait4_signal() {
    // only non-coordinator calls this function
    DCHECK(coordinator_id != 0);

    signal_in_queue.wait_till_non_empty();

    std::unique_ptr<Message> message(signal_in_queue.front());
    bool ok = signal_in_queue.pop();
    CHECK(ok);

    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = static_cast<RStoreMessage>(messagePiece.get_message_type());
    CHECK(type == RStoreMessage::SIGNAL);

    uint32_t status;
    StringPiece stringPiece = messagePiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> status;

    return static_cast<RStoreWorkerStatus>(status);
  }

  void wait4_stop(std::size_t n) {

    // wait for n stop messages

    for (auto i = 0u; i < n; i++) {

      signal_in_queue.wait_till_non_empty();

      std::unique_ptr<Message> message(signal_in_queue.front());
      bool ok = signal_in_queue.pop();
      CHECK(ok);

      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<RStoreMessage>(messagePiece.get_message_type());
      CHECK(type == RStoreMessage::SIGNAL);

      uint32_t status;
      StringPiece stringPiece = messagePiece.toStringPiece();
      Decoder dec(stringPiece);
      dec >> status;

      CHECK(status == static_cast<uint32_t>(RStoreWorkerStatus::STOP));
    }
  }

  void wait4_ack(RStoreWorkerStatus status) {

    // only coordinator waits for ack
    DCHECK(coordinator_id == 0);

    std::size_t n_coordinators = context.coordinatorNum;

    for (auto i = 0u; i < n_coordinators; i++) {
      if (i == coordinator_id) {
        continue;
      }

      ack_in_queue.wait_till_non_empty();

      std::unique_ptr<Message> message(ack_in_queue.front());
      bool ok = ack_in_queue.pop();
      CHECK(ok);

      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<RStoreMessage>(messagePiece.get_message_type());

      if (status == RStoreWorkerStatus::C_PHASE) {
        CHECK(type == RStoreMessage::C_PHASE_ACK);
      } else {
        CHECK(type == RStoreMessage::S_PHASE_ACK);
      }
    }
  }

  void broadcast_stop() {

    std::size_t n_coordinators = context.coordinatorNum;

    for (auto i = 0u; i < n_coordinators; i++) {
      if (i == coordinator_id)
        continue;
      MessageFactoryType::new_signal_message(*messages[i],
                                             RStoreWorkerStatus::STOP);
    }

    flush_messages();
  }

  void send_ack(RStoreWorkerStatus status) {

    // only non-coordinator calls this function
    DCHECK(coordinator_id != 0);

    if (status == RStoreWorkerStatus::C_PHASE) {
      MessageFactoryType::new_c_phase_ack_message(*messages[0]);
    } else if (status == RStoreWorkerStatus::S_PHASE) {
      MessageFactoryType::new_s_phase_ack_message(*messages[0]);
    } else {
      CHECK(false);
    }
    flush_messages();
  }

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

    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());

    auto message_type =
        static_cast<RStoreMessage>(messagePiece.get_message_type());

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
      CHECK(false) << "Message type: " << static_cast<uint32_t>(message_type);
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
  RandomType random;
  LockfreeQueue<Message *> ack_in_queue, signal_in_queue, out_queue;
  std::vector<std::unique_ptr<Message>> messages;

public:
  std::atomic<uint32_t> worker_status;
  std::atomic<uint32_t> n_completed_workers;
};
} // namespace scar