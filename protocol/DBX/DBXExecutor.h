//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/DBX/DBX.h"
#include "protocol/DBX/DBXHelper.h"
#include "protocol/DBX/DBXMessage.h"

#include <chrono>
#include <thread>

namespace scar {

template <class Workload> class DBXExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TableType = typename DatabaseType::TableType;
  using TransactionType = DBXTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = DBX<DatabaseType>;

  using MessageType = DBXMessage;
  using MessageFactoryType = DBXMessageFactory;
  using MessageHandlerType = DBXMessageHandler;

  DBXExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context,
              std::vector<std::unique_ptr<TransactionType>> &transactions,
              std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
              std::atomic<uint32_t> &worker_status,
              std::atomic<uint32_t> &total_abort,
              std::atomic<uint32_t> &n_complete_workers,
              std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        worker_status(worker_status), total_abort(total_abort),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, db, random, *partitioner),
        random(id), // make sure each worker has a different seed.
        protocol(db, context, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~DBXExecutor() = default;

  void start() override {

    LOG(INFO) << "DBXExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "CalvinExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::DBX_READ);

      n_started_workers.fetch_add(1);
      read_snapshot();
      n_complete_workers.fetch_add(1);
      // wait to DBX_READ
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::DBX_READ) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      reserve_transactions();
      n_complete_workers.fetch_add(1);
      // wait to DBX_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::DBX_RESERVE) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      commit_transactions();
      n_complete_workers.fetch_add(1);
      // wait to DBX_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::DBX_COMMIT) {
        std::this_thread::yield();
      }
    }
  }

  void read_snapshot() {
    // load epoch
    auto cur_epoch = epoch.load();
    auto n_abort = total_abort.load();
    for (auto i = id; i < transactions.size(); i += context.worker_num) {

      // if not null, then it's an aborted transaction from last batch.
      // reset it.
      // else generate a new transaction.

      if (transactions[i] == nullptr || i >= n_abort) {
        auto partition_id = random.uniform_dist(0, context.partition_num - 1);
        transactions[i] =
            workload.next_transaction(context, partition_id, storages[i]);
      } else {
        transactions[i]->reset();
      }

      transactions[i]->set_epoch(cur_epoch);
      transactions[i]->set_id(i + 1); // tid starts from 1
      setupHandlers(*transactions[i]);

      // run transactions
      auto result = transactions[i]->execute(id);
      n_network_size.fetch_add(transactions[i]->network_size);
      if (result == TransactionResult::ABORT_NORETRY) {
        transactions[i]->abort_no_retry = true;
      }
    }
  }

  void reserve_transactions() {
    for (auto k = id; k < transactions.size(); k += context.worker_num) {
      // reserve reads and writes
      const std::vector<DBXRWKey> &readSet = transactions[k]->readSet;
      const std::vector<DBXRWKey> &writeSet = transactions[k]->writeSet;

      // reserve reads;
      for (std::size_t i = 0u; i < readSet.size(); i++) {
        const DBXRWKey &readKey = readSet[i];
        if (readKey.get_local_index_read_bit()) {
          continue;
        }

        auto tableId = readKey.get_table_id();
        auto partitionId = readKey.get_partition_id();
        // assume a single node db
        // TODO: change it to partitioned db later on
        CHECK(partitioner->has_master_partition(partitionId));

        auto table = db.find_table(tableId, partitionId);
        std::atomic<uint64_t> &tid = table->search_metadata(readKey.get_key());
        DBXHelper::reserve_read(tid, transactions[k]->epoch,
                                transactions[k]->id);
      }

      // reserve writes
      for (std::size_t i = 0u; i < writeSet.size(); i++) {
        const DBXRWKey &writeKey = writeSet[i];
        auto tableId = writeKey.get_table_id();
        auto partitionId = writeKey.get_partition_id();
        // assume a single node db
        // TODO: change it to partitioned db later on
        CHECK(partitioner->has_master_partition(partitionId));

        auto table = db.find_table(tableId, partitionId);
        std::atomic<uint64_t> &tid = table->search_metadata(writeKey.get_key());
        DBXHelper::reserve_write(tid, transactions[k]->epoch,
                                 transactions[k]->id);

        // LOG(INFO) << "write key: " << *((int*)writeKey.get_key()) << " wts: "
        // << DBXHelper::get_wts(tid.load()) << " rts: " <<
        // DBXHelper::get_rts(tid.load()) << " epoch: " <<
        // DBXHelper::get_epoch(tid.load());
      }
    }
  }

  void analyze_dependency(TransactionType &txn, bool &waw, bool &war,
                          bool &raw) {

    waw = false;
    war = false;
    raw = false;

    const std::vector<DBXRWKey> &readSet = txn.readSet;
    const std::vector<DBXRWKey> &writeSet = txn.writeSet;

    // analyze raw

    for (std::size_t i = 0u; i < readSet.size(); i++) {
      const DBXRWKey &readKey = readSet[i];
      if (readKey.get_local_index_read_bit()) {
        continue;
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      // assume a single node db
      // TODO: change it to partitioned db later on
      CHECK(partitioner->has_master_partition(partitionId));

      auto table = db.find_table(tableId, partitionId);
      uint64_t tid = table->search_metadata(readKey.get_key()).load();
      uint64_t epoch = DBXHelper::get_epoch(tid);
      uint64_t wts = DBXHelper::get_wts(tid);
      if (epoch == txn.epoch && wts < txn.id && wts != 0) {
        raw = true;
        break;
      }
    }

    // analyze war and waw

    for (std::size_t i = 0u; i < writeSet.size(); i++) {
      const DBXRWKey &writeKey = writeSet[i];

      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      // assume a single node db
      // TODO: change it to partitioned db later on
      CHECK(partitioner->has_master_partition(partitionId));

      auto table = db.find_table(tableId, partitionId);
      uint64_t tid = table->search_metadata(writeKey.get_key()).load();
      uint64_t epoch = DBXHelper::get_epoch(tid);
      uint64_t rts = DBXHelper::get_rts(tid);
      uint64_t wts = DBXHelper::get_wts(tid);
      if (epoch == txn.epoch && rts < txn.id && rts != 0) {
        war = true;
      }
      if (epoch == txn.epoch && wts < txn.id && wts != 0) {
        waw = true;
      }
    }
  }

  void commit_transactions() {
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      if (transactions[i]->abort_no_retry) {
        n_abort_no_retry.fetch_add(1);
        continue;
      }

      bool raw = false, war = false, waw = false;
      analyze_dependency(*transactions[i], waw, war, raw);
      if (waw) {
        protocol.abort(*transactions[i], messages);
        n_abort_lock.fetch_add(1);
        continue;
      }

      if (war == false || raw == false) {
        protocol.commit(*transactions[i], messages);
        n_commit.fetch_add(1);
      } else {
        n_abort_lock.fetch_add(1);
        protocol.abort(*transactions[i], messages);
      }
    }
  }

  void setupHandlers(TransactionType &txn) {

    txn.readRequestHandler = [this](std::size_t table_id,
                                    std::size_t partition_id, std::size_t tid,
                                    uint32_t key_offset, const void *key,
                                    void *value, bool local_index_read) {
      // assume a single node db
      // TODO: change it to partitioned db later on
      CHECK(this->partitioner->has_master_partition(partition_id));
      TableType *table = db.find_table(table_id, partition_id);
      DBXHelper::read(table->search(key), value, table->value_size());
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
  }

  void onExit() override {}

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();

    if (delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                message->time)
              .count() < delay->message_delay()) {
        return nullptr;
      }
    }

    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }

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
        messageHandlers[type](messagePiece,
                              *messages[message->get_source_node_id()], *table,
                              transactions);
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &epoch, &worker_status, &total_abort;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  RandomType random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, TableType &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace scar