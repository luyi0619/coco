//
// Created by Yi Lu on 7/18/18.
//

#include <string>
#include <gtest/gtest.h>
#include "common/Encoder.h"
#include "common/FixedString.h"

TEST(TestEncoder, TestBasic) {
    scar::Encoder encoder;
    int a = 0x1234;
    double b = 123456.7890123;
    char c = 0x78;
    encoder << a << b << c;
    int a2;
    double b2;
    char c2;
    scar::Decoder dec(encoder.toStringPiece());
    dec >> a2 >> b2 >> c2;
    EXPECT_EQ(a, a2);
    EXPECT_EQ(b, b2);
    EXPECT_EQ(c, c2);
}

TEST(TestEncoder, TestString) {
    scar::Encoder encoder;
    int a = 0x1234;
    std::string s = "helloworldHELLOWORLDhelloWORLDHELLOworld";
    double c = 123456.7890123;
    std::string s2 = "helloworldHELLOWORLDhelloWORLDHELLOworld";
    encoder << a << s << c << s2;
    int a2;
    std::string s3;
    double c2;
    std::string s4;
    scar::Decoder dec(encoder.toStringPiece());
    dec >> a2 >> s3 >> c2 >> s4;
    EXPECT_EQ(a, a2);
    EXPECT_EQ(s, s3);
    EXPECT_EQ(c, c2);
    EXPECT_EQ(s2, s4);
}

TEST(TestEncoder, TestFixedString) {
    scar::Encoder encoder;
    int a = 0x1234;
    scar::FixedString<40> s = "helloworldHELLOWORLDhelloWORLDHELLOworld";
    double c = 123456.7890123;
    scar::FixedString<40> s2 = "helloworldHELLOWORLDhelloWORLDHELLOworld";
    encoder << a << s << c << s2;
    int a2;
    scar::FixedString<40> s3;
    double c2;
    scar::FixedString<40> s4;
    scar::Decoder dec(encoder.toStringPiece());
    dec >> a2 >> s3 >> c2 >> s4;
    EXPECT_EQ(a, a2);
    EXPECT_EQ(s, s3);
    EXPECT_EQ(c, c2);
    EXPECT_EQ(s2, s4);
}
