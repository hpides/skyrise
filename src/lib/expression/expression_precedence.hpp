/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#pragma once

#include <stdint.h>

namespace skyrise {

/**
 * Precedence levels for parenthesizing expression arguments, see AbstractExpression::EncloseArgument(). The order of
 * enum values is important for integer comparisons.
 */
enum class ExpressionPrecedence : uint8_t {
  kHighest = 0,
  kUnaryPredicate,
  kMultiplicationDivision,
  kAdditionSubtraction,
  kBinaryTernaryPredicate,
  kLogical
};

}  // namespace skyrise
