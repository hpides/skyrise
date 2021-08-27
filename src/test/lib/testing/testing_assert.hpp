/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "check_table_equal.hpp"

/**
 * Compare two tables with respect to OrderSensitivity and FloatComparisonMode
 */
#define EXPECT_TABLE_EQ(actual_table, expected_table, order_sensitivity, float_comparison_mode)        \
  {                                                                                                    \
    if (const auto table_difference_message =                                                          \
            CheckTableEqual(actual_table, expected_table, order_sensitivity, float_comparison_mode)) { \
      FAIL() << *table_difference_message;                                                             \
    }                                                                                                  \
  }                                                                                                    \
  static_assert(true, "End call of macro with a semicolon")

/**
 * Specialised version of EXPECT_TABLE_EQ
 */
#define EXPECT_TABLE_EQ_UNORDERED(actual_table, expected_table) \
  EXPECT_TABLE_EQ(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference)

/**
 * Specialised version of EXPECT_TABLE_EQ
 */
#define EXPECT_TABLE_EQ_ORDERED(actual_table, expected_table) \
  EXPECT_TABLE_EQ(actual_table, expected_table, OrderSensitivity::kYes, FloatComparisonMode::kAbsoluteDifference)
