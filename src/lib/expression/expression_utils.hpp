/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "abstract_expression.hpp"

namespace skyrise {

/**
 * Utility to check whether two vectors of Expressions are equal according to AbstractExpression::operator==().
 * Note that this function also considers the order of elements.
 */
bool ExpressionsEqual(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                      const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b);

/**
 * Utility to AbstractExpression::DeepCopy() a vector of expressions.
 */
std::vector<std::shared_ptr<AbstractExpression>> ExpressionsDeepCopy(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

/**
 * Create a comma-separated string with the AbstractExpression::Description(mode) of each expression.
 */
std::string ExpressionDescriptions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                   const AbstractExpression::DescriptionMode mode);

enum class ExpressionVisitation { kVisitArguments, kDoNotVisitArguments };

/**
 * Calls the passed @param visitor on each sub-expression of the @param expression.
 * The visitor returns ExpressionVisitation, indicating whether the current expression's arguments should be visited
 * as well.
 *
 * @tparam Expression   Either std::shared_ptr<AbstractExpression> or const std::shared_ptr<AbstractExpression>.
 * @tparam Visitor      Functor called with every sub expression as a param returning an ExpressionVisitation.
 */
template <typename Expression, typename Visitor>
void VisitExpression(Expression& expression, Visitor visitor) {
  // The reference wrapper bit is important, so we can manipulate the Expression even by replacing sub expressions
  std::queue<std::reference_wrapper<Expression>> expression_queue;
  expression_queue.push(expression);

  while (!expression_queue.empty()) {
    const auto& expression_reference = expression_queue.front();
    expression_queue.pop();

    if (visitor(expression_reference.Get()) == ExpressionVisitation::kVisitArguments) {
      for (auto& argument : expression_reference.Get()->parameters_) {
        expression_queue.push(argument);
      }
    }
  }
}

/**
 * @return The result DataType of a non-boolean binary expression where the operands have the specified types.
 *          For example, <float> + <long> -> <double> and (<float>, <int>, <int>) -> <float>.
 *          Division of integer types will return an integer type.
 */
DataType ExpressionCommonType(const DataType lhs, const DataType rhs);

}  // namespace skyrise
