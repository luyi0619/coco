//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaMessage.h"

#include <chrono>
#include <thread>

namespace scar {

template <class Workload> class AriaExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = AriaTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Aria<DatabaseType>;

  using MessageType = AriaMessage;
  using MessageFactoryType = AriaMessageFactory;
  using MessageHandlerType = AriaMessageHandler;

  AriaExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context,
               std::vector<std::unique_ptr<TransactionType>> &transactions,
               std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
               std::atomic<uint32_t> &lock_manager_status,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &total_abort,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        lock_manager_status(lock_manager_status), worker_status(worker_status),
        total_abort(total_abort), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, db, random, *partitioner),
        n_lock_manager(context.aria_lock_manager),
        n_workers(context.worker_num - n_lock_manager),
        lock_manager_id(AriaHelper::worker_id_to_lock_manager_id(
            id, n_lock_manager, n_workers)),
        init_transaction(false),
        random(id), // make sure each worker has a different seed.
        // random(reinterpret_cast<uint64_t >(this)),
        protocol(db, context, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~AriaExecutor() = default;

  void start() override {

    LOG(INFO) << "AriaExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "AriaExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Aria_READ);

      n_started_workers.fetch_add(1);
      // we find active coord and relevant transactions
      generate_transactions();
      read_snapshot();
      n_complete_workers.fetch_add(1);
      // wait to Aria_READ
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Aria_READ) {
        process_request();
      }
      process_request();
      n_complete_workers.fetch_add(1);

      // wait till Aria_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::Aria_COMMIT) {
        std::this_thread::yield();
      }
      n_started_workers.fetch_add(1);
      commit_transactions();
      n_complete_workers.fetch_add(1);
      // wait to Aria_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Aria_COMMIT) {
        process_request();
      }
      process_request();
      n_complete_workers.fetch_add(1);

      // wait till Aria_Fallback_Prepare
      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::Aria_Fallback_Prepare) {
        std::this_thread::yield();
      }
      n_started_workers.fetch_add(1);
      prepare_calvin_input();
      n_complete_workers.fetch_add(1);
      // wait to Aria_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Aria_Fallback_Prepare) {
        process_request();
      }
      process_request();
      n_complete_workers.fetch_add(1);

      // wait till Aria_Fallback
      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::Aria_Fallback) {
        std::this_thread::yield();
      }
      n_started_workers.fetch_add(1);
      // work as lock manager
      if (id < n_lock_manager) {
        // schedule transactions
        schedule_calvin_transactions();
      } else {
        // work as executor
        run_calvin_transactions();
      }
      n_complete_workers.fetch_add(1);
      // wait to Aria_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Aria_Fallback) {
        process_request();
      }
      process_request();
      n_complete_workers.fetch_add(1);
    }
  }

  std::size_t get_partition_id(int coord) {
    std::size_t partition_id;
    CHECK(context.partition_num % context.coordinator_num == 0);
    auto partition_num_per_node =
        context.partition_num / context.coordinator_num;
    partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                       context.coordinator_num +
                   coord;
    return partition_id;
  }

  /*
   * We run the same batch of transactions in Aria for simplicity
   * Each node generates the same set of transactions.
   * */

  void generate_transactions() {
    if (!context.same_batch || !init_transaction) {
      init_transaction = true;
      for (auto i = id; i < transactions.size(); i += context.worker_num) {
        auto partition_id = get_partition_id(i % context.coordinator_num);
        transactions[i] =
            workload.next_transaction(context, partition_id, storages[i]);
        transactions[i]->setup_process_requests_in_execution_phase();
        transactions[i]->set_id(i + 1); // tid starts from 1
        transactions[i]->set_tid_offset(i);
        transactions[i]->execution_phase = false;
        setupHandlers(*transactions[i]);
        prepare_transaction(*transactions[i], false);
      }
    } else {
      auto now = std::chrono::steady_clock::now();
      for (auto i = id; i < transactions.size(); i += context.worker_num) {
        transactions[i]->reset();
        transactions[i]->startTime = now;
      }
    }
  }

  void prepare_transaction(TransactionType &txn, bool fallback) {

    txn.setup_process_requests_in_execution_phase();
    // run execute to prepare read/write set
    auto result = txn.execute(id);
    if (result == TransactionResult::ABORT_NORETRY) {
      txn.abort_no_retry = true;
    }

    if (context.same_batch) {
      txn.save_read_count();
    }

    analyze_transaction(txn);

    if (fallback) {
      // setup handlers for execution
      txn.setup_process_requests_in_fallback_phase(n_lock_manager, n_workers,
                                                   context.coordinator_num);
      txn.execution_phase = true;
    }
  }

  void analyze_transaction(TransactionType &transaction) {

    // assuming no blind write
    auto &readSet = transaction.readSet;
    auto &active_coordinators = transaction.active_coordinators;
    active_coordinators =
        std::vector<bool>(partitioner->total_coordinators(), false);

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readkey = readSet[i];
      if (readkey.get_local_index_read_bit()) {
        continue;
      }
      auto partitionID = readkey.get_partition_id();
      if (readkey.get_write_lock_bit()) {
        active_coordinators[partitioner->master_coordinator(partitionID)] =
            true;
      }
      if (partitioner->master_coordinator(partitionID) == coordinator_id) {
        transaction.relevant = true;
      }
    }
  }

  void prepare_calvin_input() {
    // if a transaction commit, continue
    // if a transaction is not relevant, continue,
    // otherwise, we analyse the read and write set.
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      // commit in kiva
      if (transactions[i]->abort_lock == false)
        continue;
      // not relevant
      if (transactions[i]->relevant == false)
        continue;
      LOG(INFO) << "rerun " << transactions[i]->id << " in calvin.";
      transactions[i]->clear_working_set();
      prepare_transaction(*transactions[i], true);
    }
  }

  void schedule_calvin_transactions() {
    // grant locks, once all locks are acquired, assign the transaction to
    // a worker thread in a round-robin manner.
    std::size_t request_id = 0;
    for (auto i = 0u; i < transactions.size(); i++) {
      // commit in kiva
      if (transactions[i]->abort_lock == false) {
        continue;
      }
      // not relevant
      if (transactions[i]->relevant == false) {
        continue;
      }
      // do not grant locks to abort no retry transaction
      if (transactions[i]->abort_no_retry) {
        continue;
      }

      bool grant_lock = false;
      auto &readSet = transactions[i]->readSet;
      for (auto k = 0u; k < readSet.size(); k++) {
        auto &readKey = readSet[k];
        auto tableId = readKey.get_table_id();
        auto partitionId = readKey.get_partition_id();

        if (!partitioner->has_master_partition(partitionId)) {
          continue;
        }

        auto table = db.find_table(tableId, partitionId);
        auto key = readKey.get_key();

        if (readKey.get_local_index_read_bit()) {
          continue;
        }

        if (AriaHelper::partition_id_to_lock_manager_id(
                readKey.get_partition_id(), n_lock_manager,
                context.coordinator_num) != lock_manager_id) {
          continue;
        }

        grant_lock = true;
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        if (readKey.get_write_lock_bit()) {
          AriaHelper::write_lock(tid);
        } else if (readKey.get_read_lock_bit()) {
          AriaHelper::read_lock(tid);
        } else {
          CHECK(false);
        }
      }
      if (grant_lock) {
        auto worker = get_available_worker(request_id++);
        all_executors[worker]->transaction_queue.push(transactions[i].get());
      }
      // only count once
      if (i % n_lock_manager == id) {
        n_commit.fetch_add(1);
      }
    }
    set_lock_manager_bit(id);
  }

  void set_lock_manager_bit(int id) {
    uint32_t old_value, new_value;
    do {
      old_value = lock_manager_status.load();
      DCHECK(((old_value >> id) & 1) == 0);
      new_value = old_value | (1 << id);
    } while (!lock_manager_status.compare_exchange_weak(old_value, new_value));
  }

  bool get_lock_manager_bit(int id) {
    return (lock_manager_status.load() >> id) & 1;
  }

  std::size_t get_available_worker(std::size_t request_id) {
    // assume there are n lock managers and m workers
    // 0, 1, .. n-1 are lock managers
    // n, n + 1, .., n + m -1 are workers

    // the first lock managers assign transactions to n, .. , n + m/n - 1

    auto start_worker_id = n_lock_manager + n_workers / n_lock_manager * id;
    auto len = n_workers / n_lock_manager;
    return request_id % len + start_worker_id;
  }

  void run_calvin_transactions() {

    while (!get_lock_manager_bit(lock_manager_id) ||
           !transaction_queue.empty()) {

      if (transaction_queue.empty()) {
        process_request();
        continue;
      }

      TransactionType *transaction = transaction_queue.front();
      bool ok = transaction_queue.pop();
      DCHECK(ok);

      auto result = transaction->execute(id);
      n_network_size.fetch_add(transaction->network_size.load());
      if (result == TransactionResult::READY_TO_COMMIT) {
        protocol.calvin_commit(*transaction, lock_manager_id, n_lock_manager,
                               context.coordinator_num);
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - transaction->startTime)
                .count();
        percentile.add(latency);
      } else if (result == TransactionResult::ABORT) {
        // non-active transactions, release lock
        protocol.calvin_abort(*transaction, lock_manager_id, n_lock_manager,
                              context.coordinator_num);
      } else {
        CHECK(false) << "abort no retry transaction should not be scheduled.";
      }
    }
  }

  /*
   * Assume there are 2 nodes and each node has 3 threads.
   * Node A runs, 0, 2, 4, 6, 8, 10
   * Node B runs, 1, 3, 5, 7, 9, 11
   *
   * The first thread on Node A runs 0, 6
   * the second thread on Node A runs 2, 8
   */

  void read_snapshot() {
    // load epoch
    auto cur_epoch = epoch.load();
    auto n_abort = total_abort.load();
    std::size_t count = 0;
    for (auto i = id * context.coordinator_num + coordinator_id;
         i < transactions.size();
         i += context.worker_num * context.coordinator_num) {
      transactions[i]->reset();
      transactions[i]->set_epoch(cur_epoch);

      process_request();
      count++;

      // run transactions
      auto result = transactions[i]->execute(id);
      n_network_size.fetch_add(transactions[i]->network_size);
      if (result == TransactionResult::ABORT_NORETRY) {
        transactions[i]->abort_no_retry = true;
      }

      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();

    // reserve
    count = 0;
    for (auto i = id * context.coordinator_num + coordinator_id;
         i < transactions.size();
         i += context.worker_num * context.coordinator_num) {

      if (transactions[i]->abort_no_retry) {
        continue;
      }

      count++;

      // wait till all reads are processed
      while (transactions[i]->pendingResponses > 0) {
        process_request();
      }

      transactions[i]->execution_phase = true;
      // fill in writes in write set
      transactions[i]->execute(id);

      // start reservation
      reserve_transaction(*transactions[i]);
      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();
  }

  void reserve_transaction(TransactionType &txn) {

    if (context.kiva_read_only_optmization && txn.is_read_only()) {
      return;
    }

    std::vector<AriaRWKey> &readSet = txn.readSet;
    std::vector<AriaRWKey> &writeSet = txn.writeSet;

    // reserve reads;
    for (std::size_t i = 0u; i < readSet.size(); i++) {
      AriaRWKey &readKey = readSet[i];
      if (readKey.get_local_index_read_bit()) {
        continue;
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if (partitioner->has_master_partition(partitionId)) {
        std::atomic<uint64_t> &tid = AriaHelper::get_metadata(table, readKey);
        readKey.set_tid(&tid);
        AriaHelper::reserve_read(tid, txn.epoch, txn.id);
      } else {
        auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_reserve_message(
            *(this->messages[coordinatorID]), *table, txn.id, readKey.get_key(),
            txn.epoch, false);
      }
    }

    // reserve writes
    for (std::size_t i = 0u; i < writeSet.size(); i++) {
      AriaRWKey &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if (partitioner->has_master_partition(partitionId)) {
        std::atomic<uint64_t> &tid = AriaHelper::get_metadata(table, writeKey);
        writeKey.set_tid(&tid);
        AriaHelper::reserve_write(tid, txn.epoch, txn.id);
      } else {
        auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_reserve_message(
            *(this->messages[coordinatorID]), *table, txn.id,
            writeKey.get_key(), txn.epoch, true);
      }
    }
  }

  void analyze_dependency(TransactionType &txn) {

    if (context.kiva_read_only_optmization && txn.is_read_only()) {
      return;
    }

    const std::vector<AriaRWKey> &readSet = txn.readSet;
    const std::vector<AriaRWKey> &writeSet = txn.writeSet;

    // analyze raw

    for (std::size_t i = 0u; i < readSet.size(); i++) {
      const AriaRWKey &readKey = readSet[i];
      if (readKey.get_local_index_read_bit()) {
        continue;
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner->has_master_partition(partitionId)) {
        uint64_t tid = AriaHelper::get_metadata(table, readKey).load();
        uint64_t epoch = AriaHelper::get_epoch(tid);
        uint64_t wts = AriaHelper::get_wts(tid);
        DCHECK(epoch == txn.epoch);
        if (epoch == txn.epoch && wts < txn.id && wts != 0) {
          txn.raw = true;
          break;
        }
      } else {
        auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_check_message(
            *(this->messages[coordinatorID]), *table, txn.id, txn.tid_offset,
            readKey.get_key(), txn.epoch, false);
        txn.pendingResponses++;
      }
    }

    // analyze war and waw

    for (std::size_t i = 0u; i < writeSet.size(); i++) {
      const AriaRWKey &writeKey = writeSet[i];

      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner->has_master_partition(partitionId)) {
        uint64_t tid = AriaHelper::get_metadata(table, writeKey).load();
        uint64_t epoch = AriaHelper::get_epoch(tid);
        uint64_t rts = AriaHelper::get_rts(tid);
        uint64_t wts = AriaHelper::get_wts(tid);
        DCHECK(epoch == txn.epoch);
        if (epoch == txn.epoch && rts < txn.id && rts != 0) {
          txn.war = true;
        }
        if (epoch == txn.epoch && wts < txn.id && wts != 0) {
          txn.waw = true;
        }
      } else {
        auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_check_message(
            *(this->messages[coordinatorID]), *table, txn.id, txn.tid_offset,
            writeKey.get_key(), txn.epoch, true);
        txn.pendingResponses++;
      }
    }
  }

  void commit_transactions() {
    std::size_t count = 0;
    for (auto i = id * context.coordinator_num + coordinator_id;
         i < transactions.size();
         i += context.worker_num * context.coordinator_num) {
      if (transactions[i]->abort_no_retry) {
        continue;
      }

      count++;

      analyze_dependency(*transactions[i]);
      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();

    count = 0;
    for (auto i = id * context.coordinator_num + coordinator_id;
         i < transactions.size();
         i += context.worker_num * context.coordinator_num) {
      if (transactions[i]->abort_no_retry) {
        n_abort_no_retry.fetch_add(1);
        continue;
      }
      count++;

      // wait till all checks are processed
      while (transactions[i]->pendingResponses > 0) {
        process_request();
      }

      if (context.kiva_read_only_optmization &&
          transactions[i]->is_read_only()) {
        n_commit.fetch_add(1);
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - transactions[i]->startTime)
                .count();
        percentile.add(latency);
        continue;
      }

      if (transactions[i]->waw) {
        protocol.abort(*transactions[i], messages);
        n_abort_lock.fetch_add(1);
        continue;
      }

      if (context.kiva_snapshot_isolation) {
        protocol.commit(*transactions[i], messages);
        n_commit.fetch_add(1);
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - transactions[i]->startTime)
                .count();
        percentile.add(latency);
      } else {
        if (context.kiva_reordering_optmization) {
          if (transactions[i]->war == false || transactions[i]->raw == false) {
            protocol.commit(*transactions[i], messages);
            n_commit.fetch_add(1);
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() -
                    transactions[i]->startTime)
                    .count();
            percentile.add(latency);
          } else {
            n_abort_lock.fetch_add(1);
            protocol.abort(*transactions[i], messages);
          }
        } else {
          if (transactions[i]->raw) {
            n_abort_lock.fetch_add(1);
            protocol.abort(*transactions[i], messages);
          } else {
            protocol.commit(*transactions[i], messages);
            n_commit.fetch_add(1);
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() -
                    transactions[i]->startTime)
                    .count();
            percentile.add(latency);
          }
        }
      }

      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();
  }

  void setupHandlers(TransactionType &txn) {

    txn.aria_read_handler = [this, &txn](AriaRWKey &readKey, std::size_t tid,
                                         uint32_t key_offset) {
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      const void *key = readKey.get_key();
      void *value = readKey.get_value();
      bool local_index_read = readKey.get_local_index_read_bit();

      bool local_read = false;

      if (this->partitioner->has_master_partition(partition_id)) {
        local_read = true;
      }

      ITable *table = db.find_table(table_id, partition_id);
      if (local_read || local_index_read) {
        // set tid meta_data
        auto row = table->search(key);
        AriaHelper::set_key_tid(readKey, row);
        AriaHelper::read(row, value, table->value_size());
      } else {
        auto coordinatorID =
            this->partitioner->master_coordinator(partition_id);
        txn.network_size += MessageFactoryType::new_search_message(
            *(this->messages[coordinatorID]), *table, tid, txn.tid_offset, key,
            key_offset);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
      }
    };

    txn.calvin_read_handler =
        [this, &txn](std::size_t worker_id, std::size_t table_id,
                     std::size_t partition_id, std::size_t id,
                     uint32_t key_offset, const void *key, void *value) {
          auto *worker = this->all_executors[worker_id];
          if (worker->partitioner->has_master_partition(partition_id)) {
            ITable *table = worker->db.find_table(table_id, partition_id);
            AriaHelper::read(table->search(key), value, table->value_size());
            auto &active_coordinators = txn.active_coordinators;
            for (auto i = 0u; i < active_coordinators.size(); i++) {
              if (i == worker->coordinator_id || !active_coordinators[i])
                continue;
              auto sz = MessageFactoryType::new_calvin_read_message(
                  *worker->messages[i], *table, id, key_offset, value);
              txn.network_size.fetch_add(sz);
              txn.distributed_transaction = true;
            }
            txn.local_read.fetch_add(-1);
          }
        };

    txn.local_index_read_handler = [this](std::size_t table_id,
                                          std::size_t partition_id,
                                          const void *key, void *value) {
      ITable *table = this->db.find_table(table_id, partition_id);
      AriaHelper::read(table->search(key), value, table->value_size());
    };

    txn.remote_request_handler = [this](std::size_t worker_id) {
      auto *worker = this->all_executors[worker_id];
      return worker->process_request();
    };
    txn.message_flusher = [this](std::size_t worker_id) {
      auto *worker = this->all_executors[worker_id];
      worker->flush_messages();
    };
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
  }

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

  void set_all_executors(const std::vector<AriaExecutor *> &executors) {
    all_executors = executors;
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
        ITable *table = db.find_table(messagePiece.get_table_id(),
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
  std::atomic<uint32_t> &epoch, &lock_manager_status, &worker_status,
      &total_abort;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  std::size_t n_lock_manager, n_workers;
  std::size_t lock_manager_id;
  bool init_transaction;
  RandomType random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
  LockfreeQueue<TransactionType *> transaction_queue;
  std::vector<AriaExecutor *> all_executors;
};
} // namespace scar