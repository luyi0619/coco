//
// Created by Yi Lu on 8/31/18.
//

#include "core/Partitioner.h"
#include <gtest/gtest.h>
#include <vector>

TEST(TestPartitioner, TestBasic) {

  std::vector<std::vector<bool>> masterPartitions = {
      {true, false, false, false, false, false, false},
      {false, true, false, false, false, false, false},
      {false, false, true, false, false, false, false},
      {false, false, false, true, false, false, false},
      {false, false, false, false, true, false, false},
      {false, false, false, false, false, true, false},
      {false, false, false, false, false, false, true},
      {true, false, false, false, false, false, false},
      {false, true, false, false, false, false, false},
      {false, false, true, false, false, false, false}};

  std::vector<std::vector<bool>> replicationPartitions = {
      {true, true, false, false, false, false, false},
      {false, true, true, false, false, false, false},
      {false, false, true, true, false, false, false},
      {false, false, false, true, true, false, false},
      {false, false, false, false, true, true, false},
      {false, false, false, false, false, true, true},
      {true, false, false, false, false, false, true},
      {true, true, false, false, false, false, false},
      {false, true, true, false, false, false, false},
      {false, false, true, true, false, false, false}};

  int total_coordinator = 7, total_partitions = 10;
  for (int i = 0; i < total_coordinator; i++) {
    scar::HashReplicatedPartitioner<2> partitioner(i, total_coordinator);
    for (int k = 0; k < total_partitions; k++) {
      EXPECT_TRUE(k < masterPartitions.size());
      EXPECT_TRUE(partitioner.master_coordinator(k) <
                  masterPartitions[k].size());
      EXPECT_TRUE(masterPartitions[k][partitioner.master_coordinator(k)]);
      EXPECT_TRUE(i < masterPartitions[k].size());
      EXPECT_EQ(partitioner.has_master_partition(k), masterPartitions[k][i]);

      for (int j = 0; j < total_coordinator; j++) {
        EXPECT_TRUE(k < replicationPartitions.size());
        EXPECT_TRUE(j < replicationPartitions[k].size());
        EXPECT_EQ(partitioner.is_partition_replicated_on(k, j),
                  replicationPartitions[k][j]);
      }
    }
  }
}

TEST(TestPartitioner, TestRackDB) {

  std::vector<std::vector<bool>> masterPartitions = {
      {true, false, false, false}, {false, true, false, false},
      {false, false, true, false}, {false, false, false, true},
      {true, false, false, false}, {false, true, false, false},
      {false, false, true, false}, {false, false, false, true},
      {true, false, false, false}, {false, true, false, false}};

  std::vector<std::vector<bool>> replicationPartitions = {
      {true, true, false, false}, {true, true, false, false},
      {true, false, true, false}, {true, false, false, true},
      {true, false, true, false}, {true, true, false, false},
      {true, false, true, false}, {true, false, false, true},
      {true, false, false, true}, {true, true, false, false}};

  int total_coordinator = 4, total_partitions = 10;

  for (int i = 0; i < total_coordinator; i++) {
    scar::RackDBPartitioner partitioner(i, total_coordinator);
    for (int k = 0; k < total_partitions; k++) {
      EXPECT_TRUE(k < masterPartitions.size());
      EXPECT_TRUE(partitioner.master_coordinator(k) <
                  masterPartitions[k].size());
      EXPECT_TRUE(masterPartitions[k][partitioner.master_coordinator(k)]);
      EXPECT_TRUE(i < masterPartitions[k].size());
      EXPECT_EQ(partitioner.has_master_partition(k), masterPartitions[k][i]);

      for (int j = 0; j < total_coordinator; j++) {
        EXPECT_TRUE(k < replicationPartitions.size());
        EXPECT_TRUE(j < replicationPartitions[k].size());
        EXPECT_EQ(partitioner.is_partition_replicated_on(k, j),
                  replicationPartitions[k][j]);
      }
    }
  }
}