#include <iostream>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <common/FixedString.h>

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    scar::FixedString<16> str = "hello world!";
    std::cout << str << std::endl;

    return 0;
}