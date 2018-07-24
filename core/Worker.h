//
// Created by Yi Lu on 7/22/18.
//

#ifndef SCAR_WORKER_H
#define SCAR_WORKER_H

namespace scar {

template <class Database> class Worker {
public:
  using ProtocolType = typename Database::ProtocolType;

  Worker(Database &db) : db(db) {}

private:
  Database &db;
};
} // namespace scar

#endif // SCAR_WORKER_H
