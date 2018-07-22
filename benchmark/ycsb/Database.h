//
// Created by Yi Lu on 7/19/18.
//

#ifndef SCAR_YCSB_DATABASE_H
#define SCAR_YCSB_DATABASE_H

#include <vector>
#include <thread>
#include <unordered_map>
#include <chrono>
#include <glog/logging.h>
#include "core/Table.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Schema.h"
#include "benchmark/ycsb/Context.h"

namespace scar {
    namespace ycsb {
        template<class Protocol>
        class Database {
        public:
            using ProtocolType = Protocol;
            using ContextType = Context;
            using RandomType = Random;

            ITable *find_table(std::size_t table_id, std::size_t partition_id) {
                CHECK(table_id < tbl_vecs.size());
                CHECK(partition_id < tbl_vecs[table_id].size());
                return tbl_vecs[table_id][partition_id];
            }

            template<class InitFunc>
            void
            initTables(const std::string &name, InitFunc initFunc, std::size_t partitionNum, std::size_t threadsNum) {
                std::vector<std::thread> v;
                auto now = std::chrono::steady_clock::now();
                for (auto threadID = 0; threadID < threadsNum; threadID++) {
                    v.emplace_back([=]() {
                        for (auto partitionID = threadID; partitionID < partitionNum; partitionID += threadsNum) {
                            initFunc(partitionID);
                        }
                    });
                }
                for (auto &t : v) {
                    t.join();
                }
                DLOG(INFO) << name << " initialization finished in "
                           << std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - now).count() << " milliseconds.";
            }

            void initialize(const Context &context, std::size_t partitionNum, std::size_t threadsNum) {

                for (auto partitionID = 0; partitionID < partitionNum; partitionID++) {
                    auto ycsbTableID = ycsb::tableID;
                    tbl_ycsb_vec.push_back(std::make_unique<Table<1000007, ycsb::key, ycsb::value, Protocol>>(ycsbTableID));
                }

                // there is 1 table in ycsb
                tbl_vecs.resize(1);

                auto tFunc = [](std::unique_ptr<ITable> &table) {
                    return table.get();
                };

                std::transform(tbl_ycsb_vec.begin(), tbl_ycsb_vec.end(), std::back_inserter(tbl_vecs[0]),
                               tFunc);


                using std::placeholders::_1;
                initTables("ycsb", std::bind(&Database::ycsbInit, this, std::cref(context), _1), partitionNum,
                           threadsNum);
            }

        private:

            void ycsbInit(const Context &context, std::size_t partitionID) {

                Random random;
                ITable *table = tbl_ycsb_vec[partitionID].get();


                std::size_t keysPerPartition = context.keysPerPartition; // 5M keys per partition
                std::size_t partitionNum = context.partitionNum;
                std::size_t totalKeys = keysPerPartition * partitionNum;


                if (context.strategy == PartitionStrategy::RANGE) {

                    // use range partitioning

                    for (auto i = partitionID * keysPerPartition; i < (partitionID + 1) * keysPerPartition; i++) {

                        CHECK(context.getPartitionID(i) == partitionID);

                        ycsb::key key(i);
                        ycsb::value value;
                        value.Y_F01.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F02.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F03.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F04.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F05.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F06.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F07.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F08.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F09.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F10.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

                        table->insert(&key, &value);
                    }


                } else {

                    // use round-robin hash partitioning

                    for (auto i = partitionID; i < totalKeys; i += partitionNum) {

                        CHECK(context.getPartitionID(i) == partitionID);

                        ycsb::key key(i);
                        ycsb::value value;
                        value.Y_F01.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F02.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F03.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F04.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F05.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F06.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F07.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F08.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F09.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                        value.Y_F10.assignStdString(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

                        table->insert(&key, &value);
                    }
                }
            }


        private:

            std::vector<std::vector<ITable *> > tbl_vecs;
            std::vector<std::unique_ptr<ITable>> tbl_ycsb_vec;
        };
    }
}


#endif //SCAR_YCSB_DATABASE_H
