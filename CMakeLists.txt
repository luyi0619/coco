cmake_minimum_required(VERSION 3.2)
project(scar)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros -pedantic -O2")
set(CMAKE_CXX_STANDARD 14)

add_subdirectory(cmake)

# includes

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})

find_library(jemalloc_lib jemalloc) # jemalloc 5.0

# additional target to perform clang-format run, requires clang-format

# get all project files
file(GLOB_RECURSE ALL_SOURCE_FILES benchmark/*.h common/*.h core/*.h protocol/*.h test/*.cpp main.cpp)

add_custom_target(
        format
        COMMAND clang-format
        -style=LLVM
        -i
        ${ALL_SOURCE_FILES}
)

include_directories(${CMAKE_SOURCE_DIR})

file(GLOB_RECURSE COMMON_SOURCE_FILES common/*.cpp)
add_library(common STATIC ${COMMON_SOURCE_FILES})

include(CTest)

file(GLOB_RECURSE TEST_SRC_FILES test/test*.cpp)

# from list of files we'll create tests test_name.cpp -> test_name
foreach(_test_file ${TEST_SRC_FILES})
    get_filename_component(_test_name ${_test_file} NAME_WE)
    add_executable(${_test_name} ${_test_file})
    # Now simply link against gtest or gtest_main as needed. Eg
    target_link_libraries(${_test_name} common gflags glog gtest_main)
    add_test(NAME "${_test_name}" COMMAND ${_test_name})
endforeach()

add_executable(main main.cpp)
target_link_libraries(main common gflags glog)
