//
// Created by Yi Lu on 8/28/18.
//

#include "common/Encoder.h"
#include "common/Message.h"
#include <gtest/gtest.h>

TEST(TestMessage, TestBasic) {
  using coco::Decoder;
  using coco::Encoder;
  using coco::Message;
  using coco::MessagePiece;
  using coco::StringPiece;

  auto table_id = 12;
  auto partition_id = 34;
  auto source_node_id = 12;
  auto dest_node_id = 34;
  auto worker_id = 56;
  auto message_type = 12;
  int32_t message_content0 = 0x1234;
  int64_t message_content1 = 0x5678;

  Message message;
  EXPECT_TRUE(message.check_deadbeef());

  message.set_source_node_id(source_node_id);
  message.set_dest_node_id(dest_node_id);
  message.set_worker_id(worker_id);

  EXPECT_EQ(message.get_source_node_id(), source_node_id);
  EXPECT_EQ(message.get_dest_node_id(), dest_node_id);
  EXPECT_EQ(message.get_worker_id(), worker_id);

  coco::Encoder encoder(message.data);
  auto message_piece_header0 = MessagePiece::construct_message_piece_header(
      message_type, sizeof(int32_t) + MessagePiece::get_header_size(), table_id,
      partition_id);
  encoder << message_piece_header0 << message_content0;

  EXPECT_FALSE(message.check_size());
  message.flush();
  EXPECT_TRUE(message.check_size());

  auto message_piece_header1 = MessagePiece::construct_message_piece_header(
      message_type, sizeof(int64_t) + MessagePiece::get_header_size(), table_id,
      partition_id);
  encoder << message_piece_header1 << message_content1;

  EXPECT_FALSE(message.check_size());
  message.flush();
  EXPECT_TRUE(message.check_size());

  EXPECT_EQ(message.get_message_count(), 2);
  EXPECT_EQ(message.get_message_length(),
            sizeof(Message::header_type) + sizeof(Message::deadbeef_type) +
                2 * sizeof(MessagePiece::header_type) +
                sizeof(message_content0) + sizeof(message_content1));

  EXPECT_EQ(message.get_message_length(), message.data.size());

  for (auto it = message.begin(); it != message.end(); it++) {
    MessagePiece messagePiece = *it;
    EXPECT_EQ(messagePiece.get_table_id(), table_id);
    EXPECT_EQ(messagePiece.get_partition_id(), partition_id);
    EXPECT_EQ(messagePiece.get_message_type(), message_type);
  }

  auto it = message.begin();

  coco::Decoder dec((*it).toStringPiece());
  int32_t decoded_message_content0;
  dec >> decoded_message_content0;
  EXPECT_EQ(decoded_message_content0, message_content0);
  EXPECT_EQ((*it).get_message_length(),
            sizeof(MessagePiece::header_type) + sizeof(int32_t));

  it++;

  dec = coco::Decoder((*it).toStringPiece());
  int64_t decoded_message_content1;
  dec >> decoded_message_content1;
  EXPECT_EQ(decoded_message_content1, message_content1);
  EXPECT_EQ((*it).get_message_length(),
            sizeof(MessagePiece::header_type) + sizeof(int64_t));
  ++it;

  EXPECT_EQ(it, message.end());
}