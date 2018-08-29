#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "jemalloc/jemalloc.h"

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    const char *j;
    size_t s = sizeof(j);
    mallctl("version", &j, &s, NULL, 0);
    std::cout << j << std::endl;

    return 0;
}