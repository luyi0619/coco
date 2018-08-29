//
// Created by Yi Lu on 8/28/18.
//

#include "common/Encoder.h"
#include "common/Message.h"
#include <gtest/gtest.h>

TEST(TestMessage, TestBasic) {
  using scar::Message;
  Message message;

  scar::Encoder encoder(message.data);

  int a = 0x1234;
  double b = 123456.7890123;
  char c = 0x78;
  encoder << a << b << c;

  EXPECT_FALSE(message.checkSize());
  message.flush();
  EXPECT_TRUE(message.checkSize());
  EXPECT_TRUE(message.checkDeadbeef());
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

  Message message1(message.data.c_str(), message.data.length());
  EXPECT_TRUE(message1.checkSize());
  EXPECT_TRUE(message1.checkDeadbeef());
  EXPECT_EQ(message1.data.size(), sizeof(Message::size_type) +
                                      sizeof(uint32_t) + sizeof(int) +
                                      sizeof(double) + sizeof(char));
}