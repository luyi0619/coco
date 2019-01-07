//
// Created by Yi Lu on 1/2/19.
//

#include "common/Random.h"
#include <algorithm>
#include <gtest/gtest.h>
#include <iostream>
#include <unordered_map>
#include <vector>

/*
 * https://ideone.com/ZBKPrY
 *
 * table size:  1000000
 * n: 1000 990.0762255009735 997.9207184350114
 * n: 10000 9063.552104975837 9729.074428878363
 * n: 100000 43233.63847316713 59447.990505741676
 * n: 1000000 50000.44989993276 72222.56683226037
 */

const int READS = 8;
const int WRITES = 2;
const int TABLE = 1000000;
const int TRANSACTIONS = 1000000;

using namespace scar;

struct Transaction {

  Transaction(Random &random, int ts, int reads, int writes) {
    this->ts = ts;
    int n = reads + writes;
    for (int i = 0; i < n; i++) {
      bool ok;
      int record;
      do {
        ok = true;
        record = random.uniform_dist(1, TABLE);
        for (int j = 0; j < i; j++) {
          if (records[j] == record) {
            ok = false;
            break;
          }
        }
      } while (!ok);
      records.push_back(record);
      is_write.push_back(i < writes);
    }
    std::random_shuffle(is_write.begin(), is_write.end());
  }

  int ts;
  std::vector<int> records;
  std::vector<bool> is_write;
};

class Reservation {

public:
  void reserve(const Transaction &txn) {
    for (std::size_t i = 0; i < txn.records.size(); i++) {
      if (txn.is_write[i]) {
        if (writes.count(txn.records[i])) {
          writes[txn.records[i]] = std::min(writes[txn.records[i]], txn.ts);
        } else {
          writes[txn.records[i]] = txn.ts;
        }
      } else {
        if (reads.count(txn.records[i])) {
          reads[txn.records[i]] = std::min(reads[txn.records[i]], txn.ts);
        } else {
          reads[txn.records[i]] = txn.ts;
        }
      }
    }
  }

  bool raw(const Transaction &txn) {
    for (std::size_t i = 0; i < txn.records.size(); i++) {
      if (txn.is_write[i])
        continue;
      if (writes.count(txn.records[i]) && writes[txn.records[i]] < txn.ts) {
        return false;
      }
    }
    return true;
  }

  bool war(const Transaction &txn) {
    for (std::size_t i = 0; i < txn.records.size(); i++) {
      if (txn.is_write[i] == false)
        continue;
      if (reads.count(txn.records[i]) && reads[txn.records[i]] < txn.ts) {
        return false;
      }
    }
    return true;
  }

  bool waw(const Transaction &txn) {
    for (std::size_t i = 0; i < txn.records.size(); i++) {
      if (txn.is_write[i] == false)
        continue;
      if (writes.count(txn.records[i]) && writes[txn.records[i]] < txn.ts) {
        return false;
      }
    }
    return true;
  }

  bool simple_commit(const Transaction &txn) {
    for (std::size_t i = 0; i < txn.records.size(); i++) {
      if (writes.count(txn.records[i]) && writes[txn.records[i]] < txn.ts) {
        return false;
      }
    }
    return true;
  }

  bool commit(const Transaction &txn) {
    return waw(txn) && (war(txn) || raw(txn));
  }

private:
  std::unordered_map<int, int> writes, reads;
};

TEST(TestReservation, TestBasic) {

  Random random;

  std::vector<Transaction> txns;
  Reservation r;

  for (int i = 0; i < TRANSACTIONS; i++) {
    txns.push_back(Transaction(random, i, READS, WRITES));
    r.reserve(txns[i]);
  }

  int sum0 = 0, sum1 = 0;
  for (int i = 0; i < TRANSACTIONS; i++) {
    if (r.simple_commit(txns[i])) {
      sum0 += 1;
    }
    if (r.commit(txns[i])) {
      sum1 += 1;
    }
  }

  std::cout << sum0 << " " << sum1 << std::endl;
  EXPECT_TRUE(true);
}