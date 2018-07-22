cmake_minimum_required(VERSION 3.2)
project(scar)

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


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wall -Wno-long-long -Wno-unused-variable -Wno-variadic-macros -pedantic -O2")
set(CMAKE_CXX_STANDARD 14)

include_directories(${CMAKE_SOURCE_DIR})

include(CTest)

# Download and unpack googletest at configure time
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
        ${CMAKE_BINARY_DIR}/googletest-build
        EXCLUDE_FROM_ALL)

file(GLOB_RECURSE TEST_SRC_FILES test/test*.cpp)

# from list of files we'll create tests test_name.cpp -> test_name
foreach(_test_file ${TEST_SRC_FILES})
    get_filename_component(_test_name ${_test_file} NAME_WE)
    add_executable(${_test_name} ${_test_file})
    # Now simply link against gtest or gtest_main as needed. Eg
    target_link_libraries(${_test_name} gflags glog gtest_main)
    add_test(NAME "${_test_name}" COMMAND ${_test_name})
endforeach()

add_executable(main main.cpp)
target_link_libraries(main gflags glog)
