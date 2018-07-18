//
// Created by Yi Lu on 7/17/18.
//

#include <string>
#include <gtest/gtest.h>
#include "common/Serialization.h"

TEST(TestSerialization, TestBasic) {
    int a = 0x1234;
    EXPECT_EQ(scar::Deserializer<int>()(scar::Serializer<int>()(a)), a);
    double b = 123456.7890123;
    EXPECT_EQ(scar::Deserializer<double>()(scar::Serializer<double>()(b)), b);
    char c = 0x78;
    EXPECT_EQ(scar::Deserializer<char>()(scar::Serializer<char>()(c)), c);
}

TEST(TestSerialization, TestString) {
    std::string s = "helloworldHELLOWORLDhelloWORLDHELLOworld";
    EXPECT_EQ(scar::Deserializer<std::string>()(scar::Serializer<std::string>()(s)), s);
}