//
// Created by Yi Lu on 7/17/18.
//

#include "common/FixedString.h"
#include <gtest/gtest.h>

TEST(TestSerialization, TestBasic) {
  std::size_t size;
  int a1 = 0x1234, a2;
  size = scar::Deserializer<int>()(scar::Serializer<int>()(a1), a2);
  EXPECT_EQ(a1, a2);
  EXPECT_EQ(size, sizeof(int));

  double b1 = 123456.7890123, b2;
  size = scar::Deserializer<double>()(scar::Serializer<double>()(b1), b2);
  EXPECT_EQ(b1, b2);
  EXPECT_EQ(size, sizeof(double));

  double c1 = 0x78, c2;
  size = scar::Deserializer<double>()(scar::Serializer<double>()(c1), c2);
  EXPECT_EQ(c1, c2);
  EXPECT_EQ(size, sizeof(double));
}

TEST(TestSerialization, TestString) {
  std::size_t size;
  std::string s1 = "helloworldHELLOWORLDhelloWORLDHELLOworld", s2;
  size = scar::Deserializer<std::string>()(scar::Serializer<std::string>()(s1),
                                           s2);
  EXPECT_EQ(s1, s2);
  EXPECT_EQ(size, sizeof(std::string::size_type) + s1.size());
}

TEST(TestSerialization, TestFixedString) {
  std::size_t size;
  scar::FixedString<40> s1 = "helloworldHELLOWORLDhelloWORLDHELLOworld", s2;
  auto serializedString = scar::Serializer<scar::FixedString<40>>()(s1);
  size = scar::Deserializer<scar::FixedString<40>>()(serializedString, s2);
  EXPECT_EQ(serializedString.length(), 40);
  EXPECT_EQ(s1, s2);
  EXPECT_EQ(size, s1.size());

  s1 = "hello";
  EXPECT_EQ(s1.toString(), "hello" + std::string(35, ' '));
  serializedString = scar::Serializer<scar::FixedString<40>>()(s1);
  size = scar::Deserializer<scar::FixedString<40>>()(serializedString, s2);
  EXPECT_EQ(serializedString.length(), 40);
  EXPECT_EQ(s1, s2);
  EXPECT_EQ(size, s1.size());
}