//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinMessage.h"

#include <chrono>

namespace scar {

template <class Workload> class CalvinExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TableType = typename DatabaseType::TableType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Calvin<DatabaseType>;

  using MessageType = CalvinMessage;
  using MessageFactoryType = CalvinMessageFactory;
  using MessageHandlerType = CalvinMessageHandler;

  CalvinExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                 ContextType &context, std::atomic<bool> &stop_flag)
      : Worker(coordinator_id, id), db(db), context(context),
        partitioner(std::make_unique<CalvinPartitioner>(
            coordinator_id, context.coordinator_num, context.lock_manager_num,
            CalvinHelper::get_replica_group_sizes(context.replica_group))),
        stop_flag(stop_flag) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~CalvinExecutor() = default;

  void start() override {
    LOG(INFO) << "CalvinExecutor " << id << " started.";

    ProtocolType protocol(db, *partitioner);

    while (!stop_flag.load()) {

      transaction_queue.wait_till_non_empty();

      TransactionType *transaction = transaction_queue.front();
      bool ok = transaction_queue.pop();
      DCHECK(ok);

      auto result = transaction->execute();
      if (result == TransactionResult::READY_TO_COMMIT) {
        bool ok = protocol.commit(*transaction, messages);
        DCHECK(ok); // transaction in calvin must commit
        n_commit.fetch_add(1);
      } else {
        n_abort_no_retry.fetch_add(1);
      }
      flush_messages(); // push read messages
    }

    LOG(INFO) << "CalvinExecutor " << id << " exits.";
  }

  void onExit() override {}

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();
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

private:
  DatabaseType &db;
  ContextType &context;
  std::unique_ptr<Partitioner> partitioner;
  std::atomic<bool> &stop_flag;
  RandomType random;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<std::function<void(MessagePiece, Message &, TableType &,
                                 std::vector<TransactionType> &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;

  LockfreeQueue<TransactionType *> transaction_queue;
};
} // namespace scar