//
// Created by Yi Lu on 9/13/18.
//

#include "protocol/Calvin/CalvinPartitioner.h"
#include <gtest/gtest.h>

TEST(TestCalvinPartitioner, TestBasic) {

  using scar::CalvinPartitioner;

  {
    std::vector<std::size_t> replica_group_sizes = {2, 3, 4};

    CalvinPartitioner p0 = CalvinPartitioner(0, 9, 4, replica_group_sizes);
    EXPECT_EQ(p0.replica_group_id, 0);
    EXPECT_EQ(p0.replica_group_size, 2);

    CalvinPartitioner p1 = CalvinPartitioner(1, 9, 4, replica_group_sizes);
    EXPECT_EQ(p1.replica_group_id, 0);
    EXPECT_EQ(p1.replica_group_size, 2);

    CalvinPartitioner p2 = CalvinPartitioner(2, 9, 4, replica_group_sizes);
    EXPECT_EQ(p2.replica_group_id, 1);
    EXPECT_EQ(p2.replica_group_size, 3);

    CalvinPartitioner p3 = CalvinPartitioner(3, 9, 4, replica_group_sizes);
    EXPECT_EQ(p3.replica_group_id, 1);
    EXPECT_EQ(p3.replica_group_size, 3);

    CalvinPartitioner p4 = CalvinPartitioner(4, 9, 4, replica_group_sizes);
    EXPECT_EQ(p4.replica_group_id, 1);
    EXPECT_EQ(p4.replica_group_size, 3);

    CalvinPartitioner p5 = CalvinPartitioner(5, 9, 4, replica_group_sizes);
    EXPECT_EQ(p5.replica_group_id, 2);
    EXPECT_EQ(p5.replica_group_size, 4);

    CalvinPartitioner p6 = CalvinPartitioner(6, 9, 4, replica_group_sizes);
    EXPECT_EQ(p6.replica_group_id, 2);
    EXPECT_EQ(p6.replica_group_size, 4);

    CalvinPartitioner p7 = CalvinPartitioner(7, 9, 4, replica_group_sizes);
    EXPECT_EQ(p7.replica_group_id, 2);
    EXPECT_EQ(p7.replica_group_size, 4);

    CalvinPartitioner p8 = CalvinPartitioner(8, 9, 4, replica_group_sizes);
    EXPECT_EQ(p8.replica_group_id, 2);
    EXPECT_EQ(p8.replica_group_size, 4);
  }

  {
    std::vector<std::size_t> replica_group_sizes = {1, 3};

    CalvinPartitioner p0 = CalvinPartitioner(0, 4, 4, replica_group_sizes);
    EXPECT_EQ(p0.replica_group_id, 0);
    EXPECT_EQ(p0.replica_group_size, 1);

    CalvinPartitioner p1 = CalvinPartitioner(1, 4, 4, replica_group_sizes);
    EXPECT_EQ(p1.replica_group_id, 1);
    EXPECT_EQ(p1.replica_group_size, 3);

    CalvinPartitioner p2 = CalvinPartitioner(2, 4, 4, replica_group_sizes);
    EXPECT_EQ(p2.replica_group_id, 1);
    EXPECT_EQ(p2.replica_group_size, 3);

    CalvinPartitioner p3 = CalvinPartitioner(3, 4, 4, replica_group_sizes);
    EXPECT_EQ(p3.replica_group_id, 1);
    EXPECT_EQ(p3.replica_group_size, 3);
  }
}

TEST(TestCalvinPartitioner, TestShard) {

  using scar::CalvinPartitioner;

  std::vector<std::size_t> replica_group_sizes = {2, 1};

  CalvinPartitioner p0(0, 3, 4, replica_group_sizes);

  EXPECT_EQ(p0.get_shard_id(0), 0);
  EXPECT_EQ(p0.get_shard_id(1), 1);
  EXPECT_EQ(p0.get_shard_id(2), 2);
  EXPECT_EQ(p0.get_shard_id(3), 3);
  EXPECT_EQ(p0.get_shard_id(4), 0);
  EXPECT_EQ(p0.get_shard_id(5), 1);
  EXPECT_EQ(p0.get_shard_id(6), 2);
  EXPECT_EQ(p0.get_shard_id(7), 3);

  EXPECT_EQ(p0.master_coordinator(0), 0);
  EXPECT_EQ(p0.master_coordinator(1), 1);
  EXPECT_EQ(p0.master_coordinator(2), 0);
  EXPECT_EQ(p0.master_coordinator(3), 1);

  CalvinPartitioner p1(1, 3, 4, replica_group_sizes);

  EXPECT_EQ(p1.get_shard_id(0), 0);
  EXPECT_EQ(p1.get_shard_id(1), 1);
  EXPECT_EQ(p1.get_shard_id(2), 2);
  EXPECT_EQ(p1.get_shard_id(3), 3);
  EXPECT_EQ(p1.get_shard_id(4), 0);
  EXPECT_EQ(p1.get_shard_id(5), 1);
  EXPECT_EQ(p1.get_shard_id(6), 2);
  EXPECT_EQ(p1.get_shard_id(7), 3);

  EXPECT_EQ(p1.master_coordinator(0), 0);
  EXPECT_EQ(p1.master_coordinator(1), 1);
  EXPECT_EQ(p1.master_coordinator(2), 0);
  EXPECT_EQ(p1.master_coordinator(3), 1);

  CalvinPartitioner p2(2, 3, 3, replica_group_sizes);

  EXPECT_EQ(p2.get_shard_id(0), 0);
  EXPECT_EQ(p2.get_shard_id(1), 1);
  EXPECT_EQ(p2.get_shard_id(2), 2);
  EXPECT_EQ(p2.get_shard_id(3), 0);
  EXPECT_EQ(p2.get_shard_id(4), 1);
  EXPECT_EQ(p2.get_shard_id(5), 2);
  EXPECT_EQ(p2.get_shard_id(6), 0);
  EXPECT_EQ(p2.get_shard_id(7), 1);

  EXPECT_EQ(p2.master_coordinator(0), 2);
  EXPECT_EQ(p2.master_coordinator(1), 2);
  EXPECT_EQ(p2.master_coordinator(2), 2);
}