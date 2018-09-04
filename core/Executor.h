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
           ContextType &context, std::atomic<uint64_t> &epoch,
           std::atomic<bool> &stopFlag)
      : Worker(coordinator_id, id), db(db), context(context),
        partitioner(std::make_unique<HashPartitioner>(coordinator_id,
                                                      context.coordinatorNum)),
        epoch(epoch), stopFlag(stopFlag), protocol(db, epoch, *partitioner),
        workload(coordinator_id, id, db, context, random, *partitioner) {
    transactionId.store(0);

    for (auto i = 0u; i < context.coordinatorNum; i++) {
      syncMessages.emplace_back(std::make_unique<Message>());
      init_message(syncMessages[i].get(), i);
      asyncMessages.emplace_back(std::make_unique<Message>());
      init_message(asyncMessages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  void start() override {
    std::queue<std::unique_ptr<TransactionType>> q;
    StorageType storage;

    while (!stopFlag.load()) {
      process_request();
      commitTransactions(q);

      transaction = workload.nextTransaction(storage);
      setupHandlers(transaction.get());

      auto result = transaction->execute();

      if (result == TransactionResult::READY_TO_COMMIT) {
        protocol.commit(*transaction, syncMessages, asyncMessages);

        transactionId.fetch_add(1);
        q.push(std::move(transaction));
      }
    }

    commitTransactions(q, true);
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

      LOG(INFO) << "message count " << message->get_message_count()
                << " length " << message->get_message_length();

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        CHECK(type < messageHandlers.size());
        LOG(INFO) << "message type " << type << " "
                  << messagePiece.get_message_length();
        TableType *table = db.find_table(messagePiece.get_table_id(),
                                         messagePiece.get_partition_id());
        messageHandlers[type](messagePiece,
                              *syncMessages[message->get_source_node_id()],
                              *table, *transaction);
      }

      size += message->get_message_count();
      flushMessages(syncMessages);
    }
    return size;
  }

private:
  void setupHandlers(TransactionType *txn) {
    txn->readRequestHandler =
        [this](std::size_t table_id, std::size_t partition_id,
               uint32_t key_offset, const void *key, void *value) -> uint64_t {
      if (partitioner->has_master_partition(partition_id)) {
        return protocol.search(table_id, partition_id, key, value);
      } else {
        TableType *table = db.find_table(table_id, partition_id);
        auto coordinatorID = partitioner->master_coordinator(partition_id);
        MessageFactoryType::new_search_message(*syncMessages[coordinatorID],
                                               *table, key, key_offset);
        return 0;
      }
    };

    txn->remoteRequestHandler = [this]() { return process_request(); };
    txn->messageFlusher = [this]() { flushSyncMessages(); };
  };

  void flushSyncMessages() { flushMessages(syncMessages); }

  void flushAsyncMessages() { flushMessages(asyncMessages); }

  void flushMessages(std::vector<std::unique_ptr<Message>> &messages) {

    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = messages[i].release();
      LOG(INFO) << "put " << (void *)message << " dst id "
                << message->get_dest_node_id() << " count "
                << message->get_message_count() << " lentgth "
                << message->get_message_length();

      outQueue.push(message);
      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    }
  }

  void commitTransactions(std::queue<std::unique_ptr<TransactionType>> &q,
                          bool retry = false) {
    using namespace std::chrono;
    do {
      auto currentEpoch = epoch.load();
      auto now = steady_clock::now();
      while (!q.empty()) {
        const auto &ptr = q.front();
        if (ptr->commitEpoch < currentEpoch) {
          auto latency = duration_cast<milliseconds>(now - ptr->startTime);
          percentile.add(latency.count());
          q.pop();
        } else {
          break;
        }
      }
    } while (!q.empty() && retry);
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
  std::atomic<uint64_t> &epoch;
  std::atomic<bool> &stopFlag;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  Percentile<int64_t> percentile;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> syncMessages, asyncMessages;
  std::vector<std::function<void(MessagePiece, Message &, TableType &,
                                 TransactionType &)>>
      messageHandlers;
};
} // namespace scar