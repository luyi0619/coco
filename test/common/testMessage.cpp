//
// Created by Yi Lu on 8/28/18.
//

#include "common/Encoder.h"
#include "common/Message.h"
#include <gtest/gtest.h>
#include <string>

TEST(TestMessage, TestBasic) {
  using scar::Message;
  Message message;

  scar::Encoder encoder(message.data);

  int a = 0x1234;
  double b = 123456.7890123;
  char c = 0x78;
  encoder << a << b << c;
  message.size() = 3;

  EXPECT_EQ(message.size(), 3);
  EXPECT_EQ(message.deadbeef(), 0xdeadbeef);
  EXPECT_EQ(message.data.size(), sizeof(Message::size_type) + sizeof(uint32_t) +
                                     sizeof(int) + sizeof(double) +
                                     sizeof(char));

  scar::Decoder dec(message.toStringPiece());

  int a2;
  double b2;
  char c2;

  dec >> a2 >> b2 >> c2;
  EXPECT_EQ(a, a2);
  EXPECT_EQ(b, b2);
  EXPECT_EQ(c, c2);
}