/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "expression_utils.hpp"

#include <algorithm>
#include <queue>
#include <sstream>

namespace skyrise {

bool ExpressionsEqual(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                      const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b) {
  return std::equal(expressions_a.begin(), expressions_a.end(), expressions_b.begin(), expressions_b.end(),
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

}  // namespace skyrise
