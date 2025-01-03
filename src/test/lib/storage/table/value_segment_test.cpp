/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "storage/table/value_segment.hpp"

#include <gtest/gtest.h>

namespace skyrise {

class StorageValueSegmentTest : public ::testing::Test {
 protected:
  ValueSegment<int> value_segment_int_{false, 100};
  ValueSegment<std::string> value_segment_str_{false, 100};
  ValueSegment<double> value_segment_double_{false, 100};
};

TEST_F(StorageValueSegmentTest, GetSize) {
  EXPECT_EQ(value_segment_int_.Size(), 0);
  EXPECT_EQ(value_segment_str_.Size(), 0);
  EXPECT_EQ(value_segment_double_.Size(), 0);
}

TEST_F(StorageValueSegmentTest, AddValueOfSameType) {
  value_segment_int_.Append(3);
  EXPECT_EQ(value_segment_int_.Size(), 1);

  value_segment_str_.Append(std::string("Hello"));
  EXPECT_EQ(value_segment_str_.Size(), 1);

  value_segment_double_.Append(3.14);
  EXPECT_EQ(value_segment_double_.Size(), 1);
}

TEST_F(StorageValueSegmentTest, RetrieveValue) {
  value_segment_int_.Append(3);
  EXPECT_EQ(value_segment_int_.Values()[0], 3);

  value_segment_str_.Append(std::string("Hello"));
  EXPECT_EQ(value_segment_str_.Values()[0], "Hello");

  value_segment_double_.Append(3.14);
  EXPECT_EQ(value_segment_double_.Values()[0], 3.14);
}

TEST_F(StorageValueSegmentTest, AppendNullValueWhenNotNullable) {
  EXPECT_TRUE(!value_segment_int_.IsNullable());
  EXPECT_TRUE(!value_segment_str_.IsNullable());
  EXPECT_TRUE(!value_segment_double_.IsNullable());

  EXPECT_THROW(value_segment_int_.Append(kNullValue), std::exception);
  EXPECT_THROW(value_segment_str_.Append(kNullValue), std::exception);
  EXPECT_THROW(value_segment_double_.Append(kNullValue), std::exception);
}

TEST_F(StorageValueSegmentTest, AppendNullValueWhenNullable) {
  auto value_segment_int = ValueSegment<int>{true};
  auto value_segment_str = ValueSegment<std::string>{true};
  auto value_segment_double = ValueSegment<double>{true};

  EXPECT_TRUE(value_segment_int.IsNullable());
  EXPECT_TRUE(value_segment_str.IsNullable());
  EXPECT_TRUE(value_segment_double.IsNullable());

  EXPECT_NO_THROW(value_segment_int.Append(kNullValue));
  EXPECT_NO_THROW(value_segment_str.Append(kNullValue));
  EXPECT_NO_THROW(value_segment_double.Append(kNullValue));
}

TEST_F(StorageValueSegmentTest, ArraySubscriptOperatorReturnsNullValue) {
  auto value_segment_int = ValueSegment<int>{true};
  auto value_segment_str = ValueSegment<std::string>{true};
  auto value_segment_double = ValueSegment<double>{true};

  value_segment_int.Append(kNullValue);
  value_segment_str.Append(kNullValue);
  value_segment_double.Append(kNullValue);

  EXPECT_TRUE(VariantIsNull(value_segment_int[0]));
  EXPECT_TRUE(VariantIsNull(value_segment_str[0]));
  EXPECT_TRUE(VariantIsNull(value_segment_double[0]));
}

TEST_F(StorageValueSegmentTest, MemoryUsageEstimation) {
  /**
   * As ValueSegments are pre-allocated, their size should not change when inserting data, except for strings placed
   * on the heap.
   */

  const size_t empty_usage_int = value_segment_int_.MemoryUsage();
  const size_t empty_usage_double = value_segment_double_.MemoryUsage();
  const size_t empty_usage_str = value_segment_str_.MemoryUsage();

  value_segment_int_.Append(1);
  value_segment_int_.Append(2);

  const std::string short_str = "Hello";
  const std::string longer_str = "HelloWorldHaveANiceDayWithSunshineAndGoodCofefe";

  value_segment_str_.Append(short_str);
  value_segment_str_.Append(longer_str);

  value_segment_double_.Append(42.1337);

  EXPECT_EQ(empty_usage_int, value_segment_int_.MemoryUsage());
  EXPECT_EQ(empty_usage_double, value_segment_double_.MemoryUsage());

  EXPECT_GE(value_segment_str_.MemoryUsage(), empty_usage_str);
  // The short string will fit within the SSO capacity of a string and the long string will be placed on the heap.
  EXPECT_EQ(value_segment_str_.MemoryUsage(), empty_usage_str + longer_str.capacity() + 1);
}

}  // namespace skyrise
