//
// Created by Yi Lu on 7/24/18.
//
#include "common/Socket.h"
#include <gtest/gtest.h>
#include <thread>

const int N = 10;
const int port = 12345;

void listen_thread() {
  coco::Listener l("127.0.0.1", port, N);
  for (int i = 0; i < N; i++) {
    coco::Socket s = l.accept();
    int id;
    s.read_number(id);
    s.write_number(id);
    s.write_number(0xdeadbeefull);
    s.close();
  }
  l.close();
}

void send_thread(int id) {
  coco::Socket s;
  s.connect("127.0.0.1", port);
  s.write_number(id);
  uint64_t ret;
  int read_id;
  s.read_number(read_id);
  s.read_number(ret);
  EXPECT_EQ(id, read_id);
  EXPECT_EQ(ret, 0xdeadbeefull);
  s.close();
}

TEST(TestSocket, TestBasic) {
  std::vector<std::thread> v;
  v.emplace_back(std::thread(listen_thread));
  for (int i = 0; i < N; i++) {
    v.emplace_back(std::thread(send_thread, i));
  }

  for (std::size_t i = 0; i < v.size(); i++) {
    v[i].join();
  }
}