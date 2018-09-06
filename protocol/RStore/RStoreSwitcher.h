//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "core/Worker.h"
#include "protocol/rstore/RStore.h"

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
                 ContextType &context, std::atomic<bool> &stopFlag)
      : Worker(coordinator_id, id), context(context) stopFlag(stopFlag) {

    for (auto i = 0u; i < context.coordinatorNum; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }
  }

  void start() override {
    LOG(INFO) << "Switcher starts.";

    LOG(INFO) << "Switcher exits.";
  }

  std::size_t process_request() {

    std::size_t size = 0;

    while (!inQueue.empty()) {
      std::unique_ptr<Message> message(inQueue.front());
      bool ok = inQueue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = static_cast<RStoreMessage>(messagePiece.get_message_type());

        if (type == RStoreMessage::SIGNAL) {

        } else if (type == RStoreMessage::C_PHASE_ACK) {

        } else if (type == RStoreMessage::S_PHASE_ACK) {

        } else {
          DCHECK(false);
        }
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
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
  ContextType &context;
  std::atomic<bool> &stopFlag;
  RandomType random;
  std::vector<std::unique_ptr<Message>> messages;
};
} // namespace scar