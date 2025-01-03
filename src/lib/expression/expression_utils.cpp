/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "expression_utils.hpp"

#include <queue>
#include <sstream>

#include "utils/assert.hpp"

namespace skyrise {

bool ExpressionsEqual(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                      const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b) {
  return std::equal(expressions_a.cbegin(), expressions_a.cend(), expressions_b.cbegin(), expressions_b.cend(),
                    [&](const auto& expression_a, const auto& expression_b) { return *expression_a == *expression_b; });
}

std::vector<std::shared_ptr<AbstractExpression>> ExpressionsDeepCopy(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());

  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(expression->DeepCopy());
  }

  return copied_expressions;
}

std::string ExpressionDescriptions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                   const AbstractExpression::DescriptionMode mode) {
  std::stringstream stream;

  if (!expressions.empty()) {
    stream << expressions.front()->Description(mode);
  }

  for (size_t i = 1; i < expressions.size(); ++i) {
    stream << ", " << expressions[i]->Description(mode);
  }

  return stream.str();
}

DataType ExpressionCommonType(const DataType lhs, const DataType rhs) {
  Assert(lhs != DataType::kNull || rhs != DataType::kNull, "Cannot deduce common type if both sides are NULL.");
  Assert((lhs == DataType::kString) == (rhs == DataType::kString), "Strings are only compatible with strings.");

  // Long + NULL -> Long and NULL + Long -> Long
  if (lhs == DataType::kNull) {
    return rhs;
  }
  if (rhs == DataType::kNull) {
    return lhs;
  }

  if (lhs == DataType::kString) {
    return DataType::kString;
  }

  if (lhs == DataType::kDouble || rhs == DataType::kDouble) {
    return DataType::kDouble;
  }
  if (lhs == DataType::kLong) {
    return IsFloatingPointDataType(rhs) ? DataType::kDouble : DataType::kLong;
  }
  if (rhs == DataType::kLong) {
    return IsFloatingPointDataType(lhs) ? DataType::kDouble : DataType::kLong;
  }
  if (lhs == DataType::kFloat || rhs == DataType::kFloat) {
    return DataType::kFloat;
  }

  return DataType::kInt;
}

}  // namespace skyrise
