//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/Percentile.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include <chrono>
#include <thread>

namespace scar {

template <class Workload, class Protocol> class Executor : public Worker {
public:
  using WorkloadType = Workload;
  using ProtocolType = Protocol;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TableType = typename DatabaseType::TableType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = typename ProtocolType::MessageFactoryType;
  using MessageHandlerType = typename ProtocolType::MessageHandlerType;

  using StorageType = typename WorkloadType::StorageType;

  Executor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        protocol(db, context, *partitioner),
        workload(coordinator_id, db, random, *partitioner) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~Executor() = default;

  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    StorageType storage;
    uint64_t last_seed = 0;

    ExecutorStatus status;

    while ((status = static_cast<ExecutorStatus>(worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }

    n_started_workers.fetch_add(1);
    bool retry_transaction = false;

    do {
      process_request();

      if (!partitioner->is_backup()) {
        // backup node stands by for replication
        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {

          auto partition_id = get_partition_id();

          transaction =
              workload.next_transaction(context, partition_id, storage);
          setupHandlers(*transaction);
        }

        auto result = transaction->execute();
        if (result == TransactionResult::READY_TO_COMMIT) {
          bool commit = protocol.commit(*transaction, messages);
          n_network_size.fetch_add(transaction->network_size);
          if (commit) {
            n_commit.fetch_add(1);
            retry_transaction = false;
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - transaction->startTime)
                    .count();
            percentile.add(latency);
          } else {
            if (transaction->abort_lock) {
              n_abort_lock.fetch_add(1);
            } else {
              DCHECK(transaction->abort_read_validation);
              n_abort_read_validation.fetch_add(1);
            }
            random.set_seed(last_seed);
            retry_transaction = true;
            std::this_thread::sleep_for(std::chrono::microseconds(100));
          }
        } else {
          n_abort_no_retry.fetch_add(1);
        }
      }

      status = static_cast<ExecutorStatus>(worker_status.load());
    } while (status != ExecutorStatus::STOP);

    n_complete_workers.fetch_add(1);

    // once all workers are stop, we need to process the replication
    // requests

    while (static_cast<ExecutorStatus>(worker_status.load()) !=
           ExecutorStatus::CLEANUP) {
      process_request();
    }

    process_request();
    n_complete_workers.fetch_add(1);

    LOG(INFO) << "Executor " << id << " exits.";
  }

  void onExit() override {
    if (percentile.size() > 0) {
      LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
                << " us (50%) " << percentile.nth(75) << " us (75%) "
                << percentile.nth(99)
                << " us (99%), size: " << percentile.size() * sizeof(int64_t)
                << " bytes.";
    }
  }

  std::size_t get_partition_id(){

    std::size_t partition_id;

    if (context.partitioner == "pb"){
      partition_id = random.uniform_dist(0,  context.partition_num - 1);
    } else {
      auto partition_num_per_node =
          context.partition_num / context.coordinator_num;
      partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                     context.coordinator_num +
                     coordinator_id;
    }
    DCHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();
    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }

  std::size_t process_request() {

    std::size_t size = 0;

    while (!in_queue.empty()) {
      std::unique_ptr<Message> message(in_queue.front());
      bool ok = in_queue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        DCHECK(type < messageHandlers.size());
        TableType *table = db.find_table(messagePiece.get_table_id(),
                                         messagePiece.get_partition_id());

        if (type == static_cast<uint32_t>(
                        ControlMessage::OPERATION_REPLICATION_REQUEST)) {
          ControlMessageHandler::operation_replication_request_handler(
              messagePiece, *messages[message->get_source_node_id()], db, true);
        } else if (type ==
                   static_cast<uint32_t>(
                       ControlMessage::OPERATION_REPLICATION_RESPONSE)) {
          transaction->pendingResponses--;
          transaction->network_size += messagePiece.get_message_length();
        } else {
          messageHandlers[type](messagePiece,
                                *messages[message->get_source_node_id()],
                                *table, *transaction);
        }
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

  virtual void setupHandlers(TransactionType &txn) = 0;

protected:
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

protected:
  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  Percentile<int64_t> percentile;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<std::function<void(MessagePiece, Message &, TableType &,
                                 TransactionType &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace scar