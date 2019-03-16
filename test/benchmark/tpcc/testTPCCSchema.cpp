//
// Created by Yi Lu on 7/15/18.
//

#include "common/Encoder.h"

#include "benchmark/tpcc/Schema.h"
#include <gtest/gtest.h>

TEST(TestTPCCSchema, TestWarehouse) {
  scar::tpcc::warehouse::key key(1);
  EXPECT_EQ(key.W_ID, 1);
  scar::tpcc::warehouse::value value;

  value.W_YTD = 12.34;
  EXPECT_EQ(value.W_YTD, 12.34f);
  scar::tpcc::warehouse::key key_ = scar::tpcc::warehouse::key(1);
  EXPECT_EQ(key, key_);

  std::string str;
  scar::Encoder enc(str);
  enc << key << value;

  scar::tpcc::warehouse::key key1;
  scar::tpcc::warehouse::value value1;
  scar::Decoder dec(enc.toStringPiece());
  dec >> key1 >> value1;

  EXPECT_EQ(key, key1);
  EXPECT_EQ(value.W_YTD, value1.W_YTD);
}

TEST(TestTPCCSchema, TestDistrict) {
  scar::tpcc::district::key key;
  key.D_W_ID = 1;
  key.D_ID = 2;

  scar::tpcc::district::value value;

  value.D_YTD = 12.34;
  value.D_NEXT_O_ID = 3000;

  std::string str;
  scar::Encoder enc(str);

  enc << key << value;

  scar::tpcc::district::key key1;
  scar::tpcc::district::value value1;

  scar::Decoder dec(enc.toStringPiece());
  dec >> key1 >> value1;

  EXPECT_EQ(key, key1);
  EXPECT_EQ(value.D_YTD, value1.D_YTD);
  EXPECT_EQ(value.D_NEXT_O_ID, value1.D_NEXT_O_ID);
}

TEST(TestTPCCSchema, TestCustomer) {

  scar::tpcc::customer::key key;
  key.C_W_ID = 1;
  key.C_D_ID = 2;
  key.C_ID = 3;

  scar::tpcc::customer::value value;

  value.C_DATA.assign(std::string(500, '0'));
  value.C_BALANCE = 12.34;
  value.C_YTD_PAYMENT = 45.67;
  value.C_PAYMENT_CNT = 78;

  std::string str;
  scar::Encoder enc(str);

  enc << key << value;

  scar::tpcc::customer::key key1;
  scar::tpcc::customer::value value1;

  scar::Decoder dec(enc.toStringPiece());
  dec >> key1 >> value1;

  EXPECT_EQ(key, key1);
  EXPECT_EQ(value.C_DATA, value1.C_DATA);
  EXPECT_EQ(value.C_BALANCE, value1.C_BALANCE);
  EXPECT_EQ(value.C_YTD_PAYMENT, value1.C_YTD_PAYMENT);
  EXPECT_EQ(value.C_PAYMENT_CNT, value1.C_PAYMENT_CNT);
}

TEST(TestTPCCSchema, TestStock) {

  scar::tpcc::stock::key key;
  key.S_W_ID = 1;
  key.S_I_ID = 2;

  scar::tpcc::stock::value value;

  value.S_QUANTITY = 1;
  value.S_YTD = 2;
  value.S_ORDER_CNT = 21;
  value.S_REMOTE_CNT = 43;

  std::string str;
  scar::Encoder enc(str);

  enc << key << value;

  scar::tpcc::stock::key key1;
  scar::tpcc::stock::value value1;

  scar::Decoder dec(enc.toStringPiece());
  dec >> key1 >> value1;

  EXPECT_EQ(key, key1);
  EXPECT_EQ(value.S_QUANTITY, value1.S_QUANTITY);
  EXPECT_EQ(value.S_YTD, value1.S_YTD);
  EXPECT_EQ(value.S_ORDER_CNT, value1.S_ORDER_CNT);
  EXPECT_EQ(value.S_REMOTE_CNT, value1.S_REMOTE_CNT);
}