#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include <common/FixedString.h>

#include <chrono>
#include <thread>
#include <vector>

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  scar::FixedString<16> str = "hello world!";
  std::cout << str << std::endl;

  std::vector<std::thread> v;

  for (int i = 0; i < 8; i++) {
    v.emplace_back(std::thread([]() {
      long sum = 0;
      for (int i = 0; i < 10000000; i++) {
        auto start = std::chrono::steady_clock::now();
        auto stop = std::chrono::steady_clock::now();
        sum +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start)
                .count();
      }
      std::cout << sum << std::endl;
    }));
  }

  for (auto &vv : v) {
    vv.join();
  }

  return 0;
}