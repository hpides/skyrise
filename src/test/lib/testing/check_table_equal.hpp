/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "storage/table/table.hpp"

namespace skyrise {

/**
 * Indicates whether the comparison of two tables should happen order sensitive (kYes) or whether it should just be
 * checked whether both tables contain the same rows, independent of order.
 */
enum class OrderSensitivity { kYes, kNo };

/**
 * FloatComparisonMode::kRelativeDifference is better since it allows derivation independent of the absolute value.
 * When checking against manually generated tables, FloatComparisonMode::kAbsoluteDifference is the better choice.
 */
enum class FloatComparisonMode { kRelativeDifference, kAbsoluteDifference };

/**
 * Compares two tables for equality.
 *
 * @return A human-readable description of the table-mismatch, if any
 *          std::nullopt if the Tables are the same
 */
std::optional<std::string> CheckTableEqual(const std::shared_ptr<const Table>& actual_table,
                                           const std::shared_ptr<const Table>& expected_table,
                                           OrderSensitivity order_sensitivity,
                                           FloatComparisonMode float_comparison_mode);

}  // namespace skyrise
