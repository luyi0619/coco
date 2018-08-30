//
// Created by Yi Lu on 8/28/18.
//

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "common/StringPiece.h"
#include <gtest/gtest.h>

TEST(TestMessage, TestBasic) {
  using scar::Decoder;
  using scar::Encoder;
  using scar::Message;
  using scar::MessagePiece;
  using scar::StringPiece;

  auto table_id = 12;
  auto partition_id = 34;
  auto source_node_id = 12;
  auto dest_node_id = 34;
  auto worker_id = 56;
  auto message_type = 12;
  int32_t message_content = 0x1234;

  Message message;
  EXPECT_TRUE(message.checkDeadbeef());

  message.set_source_node_id(source_node_id);
  message.set_dest_node_id(dest_node_id);
  message.set_worker_id(worker_id);

  EXPECT_EQ(message.get_source_node_id(), source_node_id);
  EXPECT_EQ(message.get_dest_node_id(), dest_node_id);
  EXPECT_EQ(message.get_worker_id(), worker_id);

  scar::Encoder encoder(message.data);
  auto message_piece_header = MessagePiece::construct_message_piece_header(
      message_type, sizeof(message_content), table_id, partition_id);
  encoder << message_piece_header << message_content;

  EXPECT_FALSE(message.checkSize());
  message.flush();
  EXPECT_TRUE(message.checkSize());

  EXPECT_EQ(message.get_message_count(), 1);
  EXPECT_EQ(message.get_message_length(),
            sizeof(MessagePiece::header_type) + sizeof(message_content));

  EXPECT_EQ(message.data.size(),
            sizeof(Message::header_type) + sizeof(Message::deadbeef_type) +
                sizeof(MessagePiece::header_type) + sizeof(message_content));

  MessagePiece messagePiece(message.toStringPiece());

  EXPECT_EQ(messagePiece.get_table_id(), table_id);
  EXPECT_EQ(messagePiece.get_partition_id(), partition_id);
  EXPECT_EQ(messagePiece.get_message_type(), message_type);
  EXPECT_EQ(messagePiece.get_message_length(), sizeof(message_content));

  scar::Decoder dec(messagePiece.toStringPiece());

  int32_t decoded_message_content;
  dec >> decoded_message_content;

  EXPECT_EQ(decoded_message_content, message_content);
}