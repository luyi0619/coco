//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include <chrono>

namespace scar {

template <class Workload, class Protocol> class Executor : public Worker {
public:
  using WorkloadType = Workload;
  using ProtocolType = Protocol;
  using RWKeyType = typename WorkloadType::RWKeyType;
  using DatabaseType = typename WorkloadType::DatabaseType;
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

  Executor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           ContextType &context, std::atomic<bool> &stopFlag)
      : Worker(coordinator_id, id), db(db), context(context),
        partitioner(std::make_unique<HashReplicatedPartitioner<2>>(
            coordinator_id, context.coordinatorNum)),
        stopFlag(stopFlag), protocol(db, *partitioner),
        workload(coordinator_id, id, db, context, random, *partitioner) {

    for (auto i = 0u; i < context.coordinatorNum; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  void start() override {

    StorageType storage;
    uint64_t last_seed = 0;
    bool retry_transaction = false;

    while (!stopFlag.load()) {
      process_request();

      last_seed = random.get_seed();

      if (retry_transaction) {
        transaction->reset();
      } else {
        transaction = workload.nextTransaction(storage);
        setupHandlers(transaction.get());
      }

      auto result = transaction->execute();
      if (result == TransactionResult::READY_TO_COMMIT) {
        if (protocol.commit(*transaction, messages)) {
          n_commit.fetch_add(1);
          auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - transaction->startTime);
          percentile.add(latency.count());
          retry_transaction = false;
        } else {
          if (transaction->abort_lock) {
            n_abort_lock.fetch_add(1);
          } else {
            DCHECK(transaction->abort_read_validation);
            n_abort_read_validation.fetch_add(1);
          }
          random.set_seed(last_seed);
          retry_transaction = true;
          // std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      } else {
        n_abort_no_retry.fetch_add(1);
      }
    }

    LOG(INFO) << "Worker " << id << " exits.";
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << "ms (50%) " << percentile.nth(75) << "ms (75%) "
              << percentile.nth(99.9)
              << "ms (99.9%), size: " << percentile.size() * sizeof(int64_t)
              << " bytes.";
  }

  std::size_t process_request() {

    std::size_t size = 0;

    while (!inQueue.empty()) {
      std::unique_ptr<Message> message(inQueue.front());
      bool ok = inQueue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        DCHECK(type < messageHandlers.size());
        TableType *table = db.find_table(messagePiece.get_table_id(),
                                         messagePiece.get_partition_id());
        messageHandlers[type](messagePiece,
                              *messages[message->get_source_node_id()], *table,
                              *transaction);
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

private:
  void setupHandlers(TransactionType *txn) {
    txn->readRequestHandler =
        [this](std::size_t table_id, std::size_t partition_id,
               uint32_t key_offset, const void *key, void *value,
               bool local_index_read) -> uint64_t {
      if (partitioner->has_master_partition(partition_id) || local_index_read) {
        return protocol.search(table_id, partition_id, key, value);
      } else {
        TableType *table = db.find_table(table_id, partition_id);
        auto coordinatorID = partitioner->master_coordinator(partition_id);
        MessageFactoryType::new_search_message(*messages[coordinatorID], *table,
                                               key, key_offset);
        return 0;
      }
    };

    txn->remoteRequestHandler = [this]() { return process_request(); };
    txn->messageFlusher = [this]() { flush_messages(); };
  };

  void flush_messages() {

    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = messages[i].release();

      outQueue.push(message);
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
  DatabaseType &db;
  ContextType &context;
  std::unique_ptr<Partitioner> partitioner;
  std::atomic<bool> &stopFlag;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  Percentile<int64_t> percentile;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<std::function<void(MessagePiece, Message &, TableType &,
                                 TransactionType &)>>
      messageHandlers;
};
} // namespace scar