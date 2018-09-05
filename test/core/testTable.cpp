//
// Created by Yi Lu on 9/5/18.
//

#include "benchmark/tpcc/Schema.h"
#include "common/ClassOf.h"
#include "core/Table.h"
#include <gtest/gtest.h>

TEST(TestTable, TestTPCC) {

  using namespace scar;
  using namespace tpcc;
  using MetaDataType = std::atomic<uint64_t>;

  auto warehouse_table_id = warehouse::tableID;
  std::unique_ptr<ITable<MetaDataType>> warehouse_table = std::make_unique<
      Table<1, warehouse::key, warehouse::value, MetaDataType>>(
      warehouse_table_id, 0);

  EXPECT_EQ(warehouse_table->field_size(), sizeof(warehouse::value::W_YTD));

  auto district_table_id = district::tableID;

  std::unique_ptr<ITable<MetaDataType>> district_table =
      std::make_unique<Table<1, district::key, district::value, MetaDataType>>(
          district_table_id, 0);

  EXPECT_EQ(district_table->field_size(),
            sizeof(district::value::D_YTD) +
                sizeof(district::value::D_NEXT_O_ID));

  auto customer_table_id = customer::tableID;

  std::unique_ptr<ITable<MetaDataType>> customer_table =
      std::make_unique<Table<1, customer::key, customer::value, MetaDataType>>(
          customer_table_id, 0);

  EXPECT_EQ(customer_table->field_size(),
            ClassOf<decltype(customer::value::C_DATA)>::size() +
                sizeof(customer::value::C_BALANCE) +
                sizeof(customer::value::C_YTD_PAYMENT) +
                sizeof(customer::value::C_PAYMENT_CNT));

  auto stock_table_id = stock::tableID;

  std::unique_ptr<ITable<MetaDataType>> stock_table =
      std::make_unique<Table<1, stock::key, stock::value, MetaDataType>>(
          stock_table_id, 0);

  EXPECT_EQ(stock_table->field_size(), sizeof(stock::value::S_QUANTITY) +
                                           sizeof(stock::value::S_YTD) +
                                           sizeof(stock::value::S_ORDER_CNT) +
                                           sizeof(stock::value::S_REMOTE_CNT));

}