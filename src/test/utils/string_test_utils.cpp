#include <regex>

#include <gtest/gtest.h>

namespace {

std::string RedactMemoryAddresses(const std::string& input) {
  return std::regex_replace(input, std::regex{"0x[0-9A-Fa-f]{4,}"}, "0x00000000");
}

}  // namespace

namespace skyrise {

/**
 * Tests for the utility functions declared above.
 */
class StringTestUtils : public ::testing::Test {};

TEST_F(StringTestUtils, RedactMemoryAddresses) {
  EXPECT_EQ(RedactMemoryAddresses("0x023a010"), "0x00000000");
  EXPECT_EQ(RedactMemoryAddresses("Two addresses: 0x023a010, 0x02312"), "Two addresses: 0x00000000, 0x00000000");
}

}  // namespace skyrise
