/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "lqp_expression_utils.hpp"

#include <algorithm>
#include <queue>
#include <sstream>

#include "expression/abstract_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "lqp_utils.hpp"
#include "utils/assert.hpp"

namespace {

using namespace skyrise;  // NOLINT(google-build-using-namespace)

bool ExpressionIsNullableOnLqpImpl(const std::shared_ptr<AbstractExpression>& expression, const AbstractLqpNode& lqp) {
  const auto arguments_nullable = [&]() {
    return std::any_of(
        expression->arguments_.begin(), expression->arguments_.end(),
        [&](const auto& expression_argument) { return ExpressionIsNullableOnLqp(expression_argument, lqp); });
  };

  switch (expression->type_) {
    case ExpressionType::kAggregate: {
      const auto& aggregate_expression = static_cast<const AggregateExpression&>(*expression);
      // Aggregates (except COUNT and COUNT DISTINCT) will return NULL when executed on an
      // empty group - thus they are always nullable
      return aggregate_expression.aggregate_function_ != AggregateFunction::kCount &&
             aggregate_expression.aggregate_function_ != AggregateFunction::kCountDistinct;
    }
    case ExpressionType::kArithmetic: {
      const auto& arithmetic_expression = static_cast<const ArithmeticExpression&>(*expression);
      // We return NULL for divisions/modulo by 0
      return arguments_nullable() || arithmetic_expression.arithmetic_operator_ == ArithmeticOperator::kDivision ||
             arithmetic_expression.arithmetic_operator_ == ArithmeticOperator::kModulo;
    }
    case ExpressionType::kLqpColumn:
      Fail("This should have been forwarded to StoredTableNode/StaticTableNode/MockNode by ExpressionIsNullableOnLqp");
    case ExpressionType::kPqpColumn:
      Fail("Nullability should never be queried from a PQP Column.");
    case ExpressionType::kPredicate: {
      if (std::dynamic_pointer_cast<IsNullExpression>(expression)) {
        // IS NULL always returns a boolean value, never NULL
        return false;
      }
      return arguments_nullable();
    }
    case ExpressionType::kValue: {
      const auto& value_expression = static_cast<const ValueExpression&>(*expression);
      return VariantIsNull(value_expression.value_);
    }
    case ExpressionType::kCast:    /* fallthrough */
    case ExpressionType::kExtract: /* fallthrough */
    case ExpressionType::kList:    /* fallthrough */
    case ExpressionType::kLogical: /* fallthrough */
    case ExpressionType::kUnaryMinus:
      return arguments_nullable();
    default:
      Fail("Unhandled ExpressionType.");
  }
}

}  // namespace

namespace skyrise {

bool IsCountStarAggregateExpression(const std::shared_ptr<AbstractExpression> expression) {
  // COUNT(*) is represented by an AggregateExpression with the COUNT function and an kInvalidColumnId.
  if (expression->type_ != ExpressionType::kAggregate) {
    return false;
  }
  const auto& aggregate_expression = static_cast<const AggregateExpression&>(*expression);

  if (aggregate_expression.aggregate_function_ != AggregateFunction::kCount) {
    return false;
  }

  switch (aggregate_expression.Argument()->type_) {
    case ExpressionType::kPqpColumn: {
      const auto& pqp_column_expression = static_cast<PqpColumnExpression&>(*aggregate_expression.Argument());
      if (pqp_column_expression.column_id_ != kInvalidColumnId) {
        return false;
      }
    } break;
    case ExpressionType::kLqpColumn: {
      const auto& lqp_column_expression = static_cast<LqpColumnExpression&>(*aggregate_expression.Argument());
      if (lqp_column_expression.original_column_id_ != kInvalidColumnId) {
        return false;
      }
    } break;
    default:
      Fail("Unexpected AggregateExpression argument type.");
  }

  return true;
}

bool ExpressionsEqualToExpressionsInDifferentLqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_left,
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_right, const LqpNodeMapping& node_mapping) {
  if (expressions_left.size() != expressions_right.size()) {
    return false;
  }

  for (size_t i = 0; i < expressions_left.size(); ++i) {
    const auto& expression_left = *expressions_left[i];
    const auto& expression_right = *expressions_right[i];

    if (!ExpressionEqualToExpressionInDifferentLqp(expression_left, expression_right, node_mapping)) {
      return false;
    }
  }

  return true;
}

bool ExpressionEqualToExpressionInDifferentLqp(const AbstractExpression& expression_left,
                                               const AbstractExpression& expression_right,
                                               const LqpNodeMapping& node_mapping) {
  /**
   * Compare expression_left to expression_right by creating a deep copy of expression_left and adapting it to the LQP
   * of expression_right, then perform a normal comparison of two expressions in the same LQP.
   */

  auto copied_expression_left = expression_left.DeepCopy();
  ExpressionAdaptToDifferentLqp(copied_expression_left, node_mapping);
  return *copied_expression_left == expression_right;
}

std::vector<std::shared_ptr<AbstractExpression>> ExpressionsCopyAndAdaptToDifferentLqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const LqpNodeMapping& node_mapping) {
  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());

  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(ExpressionCopyAndAdaptToDifferentLqp(*expression, node_mapping));
  }

  return copied_expressions;
}

std::shared_ptr<AbstractExpression> ExpressionCopyAndAdaptToDifferentLqp(const AbstractExpression& expression,
                                                                         const LqpNodeMapping& node_mapping) {
  auto copied_expression = expression.DeepCopy();
  ExpressionAdaptToDifferentLqp(copied_expression, node_mapping);
  return copied_expression;
}

void ExpressionAdaptToDifferentLqp(std::shared_ptr<AbstractExpression>& expression,
                                   const LqpNodeMapping& node_mapping) {
  VisitExpression(expression, [&](auto& expression_ptr) {
    if (expression_ptr->type_ != ExpressionType::kLqpColumn) {
      return ExpressionVisitation::kVisitArguments;
    }

    const auto lqp_column_expression_ptr = std::dynamic_pointer_cast<LqpColumnExpression>(expression_ptr);
    Assert(lqp_column_expression_ptr, "Asked to adapt expression in LQP, but encountered non-LQP ColumnExpression");

    expression_ptr = ExpressionAdaptToDifferentLqp(*lqp_column_expression_ptr, node_mapping);

    return ExpressionVisitation::kDoNotVisitArguments;
  });
}

std::shared_ptr<LqpColumnExpression> ExpressionAdaptToDifferentLqp(const LqpColumnExpression& lqp_column_expression,
                                                                   const LqpNodeMapping& node_mapping) {
  const auto node = lqp_column_expression.original_node_.lock();
  Assert(node, "LqpColumnExpression is expired");
  const auto node_mapping_iter = node_mapping.find(node);
  Assert(node_mapping_iter != node_mapping.end(),
         "Couldn't find referenced node (" + node->Description() + ") in NodeMapping");

  return std::make_shared<LqpColumnExpression>(node_mapping_iter->second, lqp_column_expression.original_column_id_);
}

bool ExpressionEvaluableOnLqp(const std::shared_ptr<AbstractExpression>& expression, const AbstractLqpNode& lqp) {
  auto evaluable = true;

  VisitExpression(expression, [&](const auto& sub_expression) {
    if (lqp.FindColumnId(*sub_expression)) {
      return ExpressionVisitation::kDoNotVisitArguments;
    }

    if (IsCountStarAggregateExpression(sub_expression)) {
      // COUNT(*) needs special treatment. Because its argument is the invalid column id, it is not part of any node's
      // output_expressions. Check if sub_expression is COUNT(*) - if yes, ignore the kInvalidColumnId and verify that
      // its original_node is part of lqp.
      const auto& aggregate_expression = static_cast<const AggregateExpression&>(*sub_expression);
      const auto& lqp_column_expression = static_cast<const LqpColumnExpression&>(*aggregate_expression.Argument());
      const auto& original_node = lqp_column_expression.original_node_.lock();
      Assert(original_node, "LqpColumnExpression is expired, LQP is invalid");

      // Now check if lqp contains that original_node
      evaluable = false;
      VisitLqp(lqp.SharedFromBase(), [&](const auto& sub_lqp) {
        if (sub_lqp == original_node) {
          evaluable = true;
        }
        return LqpVisitation::kVisitInputs;
      });

      return ExpressionVisitation::kDoNotVisitArguments;
    }

    if (sub_expression->type_ == ExpressionType::kLqpColumn) {
      evaluable = false;
    }

    return ExpressionVisitation::kVisitArguments;
  });

  return evaluable;
}

bool ExpressionIsNullableOnLqp(const std::shared_ptr<AbstractExpression>& expression, const AbstractLqpNode& lqp) {
  const auto node_column_id = lqp.FindColumnId(*expression);
  if (node_column_id) {
    return lqp.IsColumnNullable(*node_column_id);
  }

  return ExpressionIsNullableOnLqpImpl(expression, lqp);
}

}  // namespace skyrise
