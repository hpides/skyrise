#include "utils/string.hpp"

#include <gtest/gtest.h>

namespace skyrise {

class StringUtilsTest : public ::testing::Test {};

TEST_F(StringUtilsTest, TrimSourceFilePath) {
  EXPECT_EQ(TrimSourceFilePath("/home/user/checkout/src/file.cpp"), "src/file.cpp");
  EXPECT_EQ(TrimSourceFilePath("hello/file.cpp"), "hello/file.cpp");
}

TEST_F(StringUtilsTest, SplitStringByDelimiter) {
  {
    const auto substrings = SplitStringByDelimiter("", '|');
    EXPECT_EQ(substrings.size(), 0);
  }
  {
    const auto substrings = SplitStringByDelimiter("a", '|');
    ASSERT_EQ(substrings.size(), 1);
    EXPECT_EQ(substrings.at(0), "a");
  }
  {
    const auto substrings = SplitStringByDelimiter("a|b", '|');
    ASSERT_EQ(substrings.size(), 2);
    EXPECT_EQ(substrings.at(0), "a");
    EXPECT_EQ(substrings.at(1), "b");
  }
  {
    const auto substrings = SplitStringByDelimiter("int|float|float", '|');
    ASSERT_EQ(substrings.size(), 3);
    EXPECT_EQ(substrings.at(0), "int");
    EXPECT_EQ(substrings.at(1), "float");
    EXPECT_EQ(substrings.at(2), "float");
  }
}

}  // namespace skyrise
