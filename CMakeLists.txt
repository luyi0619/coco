cmake_minimum_required(VERSION 3.2)
project(scar)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros -pedantic")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros -pedantic -O2")
set(CMAKE_CXX_STANDARD 14)

add_subdirectory(cmake)

find_library(jemalloc_lib jemalloc) # jemalloc 5.0
find_package(glog REQUIRED)
find_package(gflags REQUIRED)

# additional target to perform clang-format run, requires clang-format

# get all project files
file(GLOB_RECURSE ALL_SOURCE_FILES benchmark/*.h common/*.h core/*.h protocol/*.h test/*.cpp bench*.cpp)

add_custom_target(
        format
        COMMAND clang-format
        -style=LLVM
        -i
        -sort-includes
        ${ALL_SOURCE_FILES}
)

include_directories(${CMAKE_SOURCE_DIR})

file(GLOB_RECURSE COMMON_SOURCE_FILES common/*.cpp)
add_library(common STATIC ${COMMON_SOURCE_FILES})

target_link_libraries(common ${jemalloc_lib} glog gflags)

include(CTest)

file(GLOB_RECURSE TEST_SRC_FILES test/test*.cpp)

# from list of files we'll create tests test_name.cpp -> test_name
foreach(_test_file ${TEST_SRC_FILES})
    get_filename_component(_test_name ${_test_file} NAME_WE)
    add_executable(${_test_name} ${_test_file})
    # Now simply link against gtest or gtest_main as needed. Eg
    target_link_libraries(${_test_name} common gtest_main)
    add_test(NAME "${_test_name}" COMMAND ${_test_name})
endforeach()

add_executable(bench_tpcc bench_tpcc.cpp)
target_link_libraries(bench_tpcc common)

add_executable(bench_ycsb bench_ycsb.cpp)
target_link_libraries(bench_ycsb common)