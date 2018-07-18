//
// Created by Yi Lu on 7/17/18.
//

#include <string>
#include <gtest/gtest.h>
#include "common/Serialization.h"
#include "common/FixedString.h"

TEST(TestSerialization, TestBasic) {
    std::size_t size;
    int a = 0x1234;
    EXPECT_EQ(scar::Deserializer<int>()(scar::Serializer<int>()(a), size), a);
    EXPECT_EQ(size, sizeof(int));
    double b = 123456.7890123;
    EXPECT_EQ(scar::Deserializer<double>()(scar::Serializer<double>()(b), size), b);
    EXPECT_EQ(size, sizeof(double));
    char c = 0x78;
    EXPECT_EQ(scar::Deserializer<char>()(scar::Serializer<char>()(c), size), c);
    EXPECT_EQ(size, sizeof(char));
}

TEST(TestSerialization, TestString) {
    std::size_t size;
    std::string s = "helloworldHELLOWORLDhelloWORLDHELLOworld";
    EXPECT_EQ(scar::Deserializer<std::string>()(scar::Serializer<std::string>()(s), size), s);
    EXPECT_EQ(size, sizeof(std::string::size_type) + s.size());
}

TEST(TestSerialization, TestFixedString) {
    std::size_t size;
    scar::FixedString<40> s = "helloworldHELLOWORLDhelloWORLDHELLOworld";
    auto serializedString = scar::Serializer<scar::FixedString<40>>()(s);
    EXPECT_EQ(serializedString.length(), 40 + sizeof(scar::FixedString<40>::size_type));
    EXPECT_EQ(scar::Deserializer<scar::FixedString<40>>()(serializedString, size), s);
    EXPECT_EQ(size, sizeof(scar::FixedString<40>::size_type) + s.size());
}