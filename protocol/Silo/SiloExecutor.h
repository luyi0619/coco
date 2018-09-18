//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "core/Executor.h"
#include "protocol/Silo/Silo.h"

namespace scar {
template <class Workload>
class SiloExecutor
    : public Executor<Workload, Silo<typename Workload::DatabaseType>> {
public:
  using base_type = Executor<Workload, Silo<typename Workload::DatabaseType>>;

  using WorkloadType = Workload;
  using ProtocolType = Silo<typename Workload::DatabaseType>;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TableType = typename DatabaseType::TableType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = typename ProtocolType::MessageFactoryType;
  using MessageHandlerType = typename ProtocolType::MessageHandlerType;

  using StorageType = typename WorkloadType::StorageType;

  SiloExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context, std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers)
      : base_type(coordinator_id, id, db, context, worker_status,
                  n_complete_workers, n_started_workers) {}

  ~SiloExecutor() = default;

  void setupHandlers(TransactionType &txn) override {
    txn.readRequestHandler =
        [this, &txn](std::size_t table_id, std::size_t partition_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read) -> uint64_t {
      if (this->partitioner->has_master_partition(partition_id) ||
          local_index_read) {
        return this->protocol.search(table_id, partition_id, key, value);
      } else {
        TableType *table = this->db.find_table(table_id, partition_id);
        auto coordinatorID =
            this->partitioner->master_coordinator(partition_id);
        txn.network_size += MessageFactoryType::new_search_message(
            *(this->messages[coordinatorID]), *table, key, key_offset);
        return 0;
      }
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
  };
};
} // namespace scar
