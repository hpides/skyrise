#include "parquet_expression.hpp"

#include <numeric>
#include <variant>

#include <arrow/array/array_binary.h>
#include <arrow/dataset/scanner.h>

#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "storage/backend/abstract_storage.hpp"

namespace skyrise {

/**
 * @brief Converts Skyrise's logical expression (kAnd and kOr) into the respective Arrow expression
 *
 * @param expression Pointer to Skyrise's LogicalExpression
 * @return arrow::compute::Expression
 */
arrow::compute::Expression ConvertLogicalExpressionToArrowExpression(std::shared_ptr<LogicalExpression> expression) {
  const auto left = CreateArrowExpression(expression->LeftOperand());
  const auto right = CreateArrowExpression(expression->RightOperand());
  switch (expression->logical_operator_) {
    case LogicalOperator::kOr: {
      return arrow::compute::or_(left, right);
    }
    case LogicalOperator::kAnd: {
      return arrow::compute::and_(left, right);
    }
  }
  Fail("Unsupported logical expression (needs to be either kAnd or kOr).");
}

/**
 * @brief Converts Skyrise's binary expression into the respective Arrow expression
 *
 * @tparam ArrowFunction The Arrow binary function type (e.g. arrow::compute::greater)
 * @param arrow_function The Arrow binary function (e.g. arrow::compute::greater)
 * @param expression Pointer to Skyrise's AbstractPredicateExpression
 * @return arrow::compute::Expression
 */
template <typename ArrowFunction>
arrow::compute::Expression ConvertBinaryPredicateExpressionToArrowExpression(
    ArrowFunction&& arrow_function, std::shared_ptr<AbstractPredicateExpression> expression) {
  const auto expression_ = std::static_pointer_cast<BinaryPredicateExpression>(expression);
  const auto left = CreateArrowExpression(expression_->LeftOperand());
  const auto right = CreateArrowExpression(expression_->RightOperand());
  return arrow_function(left, right);
}

/**
 * @brief Converts Skyrise's binary in-between expression into the respective Arrow expression. It
 * consists of a lower function representing the lower bound and and upper function representing the
 * upper bound of the in-between function.
 *
 * @tparam ArrowLowerFunction The Arrow lower bound binary function type (e.g. arrow::compute::greater)
 * @tparam ArrowUpperFunction The Arrow upper bound binary function type (e.g. arrow::compute::smaller)
 * @param arrow_lower_function The Arrow lower bound binary function (e.g. arrow::compute::greater)
 * @param arrow_upper_function The Arrow upper bound binary function type (e.g. arrow::compute::smaller)
 * @param expression Pointer to Skyrise's AbstractPredicateExpression
 * @return arrow::compute::Expression
 */
template <typename ArrowLowerFunction, typename ArrowUpperFunction>
arrow::compute::Expression ConvertInBetweenPredicateExpressionToArrowExpression(
    ArrowLowerFunction&& arrow_lower_function, ArrowUpperFunction&& arrow_upper_function,
    std::shared_ptr<AbstractPredicateExpression> expression) {
  const auto expression_ = std::static_pointer_cast<BetweenExpression>(expression);
  const auto lower_bound = CreateArrowExpression(expression_->LowerBound());
  const auto upper_bound = CreateArrowExpression(expression_->UpperBound());
  const auto value = CreateArrowExpression(expression_->Value());
  return arrow::compute::and_(arrow_lower_function(value, lower_bound), arrow_upper_function(value, upper_bound));
}

/**
 * @brief Converts Skyrise's is-null expression into the respective Arrow Expression
 *
 * @tparam ArrowFunction The Arrow unary function type (e.g. arrow::compute::is_null)
 * @param arrow_function The Arrow unary function (e.g. arrow::compute::is_null)
 * @param expression Pointer to Skyrise's AbstractPredicateExpression
 * @return arrow::compute::Expression
 */
template <typename ArrowFunction>
arrow::compute::Expression ConvertIsNullExpressionToArrowExpression(
    ArrowFunction&& arrow_function, std::shared_ptr<AbstractPredicateExpression> expression) {
  const auto expression_ = std::static_pointer_cast<IsNullExpression>(expression);
  const auto operand = CreateArrowExpression(expression_->Operand());
  return arrow_function(operand, false);
}

/**
 * @brief  Converts several of Skyrise's abstract expression into the respective Arrow Expression e.g
 * PredicateCondition::kNotEqual to arrow::compute::not_equal
 *
 * @param expression Pointer to Skyrise's AbstractPredicateExpression
 * @return arrow::compute::Expression
 */
arrow::compute::Expression ConvertAbstractPredicateExpressionToArrowExpression(
    std::shared_ptr<AbstractPredicateExpression> expression) {
  switch (expression->predicate_condition_) {
    case PredicateCondition::kEquals: {
      return ConvertBinaryPredicateExpressionToArrowExpression(arrow::compute::equal, expression);
    }
    case PredicateCondition::kNotEquals: {
      return ConvertBinaryPredicateExpressionToArrowExpression(arrow::compute::not_equal, expression);
    }
    case PredicateCondition::kLessThan: {
      return ConvertBinaryPredicateExpressionToArrowExpression(arrow::compute::less, expression);
    }
    case PredicateCondition::kLessThanEquals: {
      return ConvertBinaryPredicateExpressionToArrowExpression(arrow::compute::less_equal, expression);
    }
    case PredicateCondition::kGreaterThan: {
      return ConvertBinaryPredicateExpressionToArrowExpression(arrow::compute::greater, expression);
    }
    case PredicateCondition::kGreaterThanEquals: {
      return ConvertBinaryPredicateExpressionToArrowExpression(arrow::compute::greater_equal, expression);
    }
    case PredicateCondition::kBetweenInclusive: {
      return ConvertInBetweenPredicateExpressionToArrowExpression(arrow::compute::greater_equal,
                                                                  arrow::compute::less_equal, expression);
    }
    case PredicateCondition::kBetweenLowerExclusive: {
      return ConvertInBetweenPredicateExpressionToArrowExpression(arrow::compute::greater, arrow::compute::less_equal,
                                                                  expression);
    }
    case PredicateCondition::kBetweenUpperExclusive: {
      return ConvertInBetweenPredicateExpressionToArrowExpression(arrow::compute::greater_equal, arrow::compute::less,
                                                                  expression);
    }
    case PredicateCondition::kBetweenExclusive: {
      return ConvertInBetweenPredicateExpressionToArrowExpression(arrow::compute::greater, arrow::compute::less,
                                                                  expression);
    }
    case PredicateCondition::kIsNull: {
      return ConvertIsNullExpressionToArrowExpression(arrow::compute::is_null, expression);
    }
    case PredicateCondition::kIsNotNull: {
      const auto not_null_expression = [](auto expression_, bool nan_is_null) {
        return arrow::compute::not_(arrow::compute::is_null(expression_, nan_is_null));
      };
      return ConvertIsNullExpressionToArrowExpression(not_null_expression, expression);
    }
    default:
      Fail("Unsupported predicate condition (cannot be one of kIn,kNotIn, kLike or kNotLike).");
  }
}

arrow::compute::Expression CreateArrowExpression(std::shared_ptr<AbstractExpression> expression) {
  switch (expression->type_) {
    case ExpressionType::kPqpColumn: {
      const auto column_expression = std::static_pointer_cast<PqpColumnExpression>(expression);
      return arrow::compute::field_ref(column_expression->column_name_);
    }
    case ExpressionType::kValue: {
      const auto value_expression = std::static_pointer_cast<ValueExpression>(expression);
      arrow::compute::Expression arrow_expression;
      ResolveDataType(DataTypeFromAllTypeVariant(value_expression->value_), [&](auto type) {
        using ColumnType = decltype(type);
        const auto value = std::get<ColumnType>(value_expression->value_);
        arrow_expression = arrow::compute::literal(value);
      });
      return arrow_expression;
    }
    case ExpressionType::kPredicate: {
      const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(expression);
      return ConvertAbstractPredicateExpressionToArrowExpression(predicate_expression);
    }
    case ExpressionType::kLogical: {
      const auto logical_expression = std::static_pointer_cast<LogicalExpression>(expression);
      return ConvertLogicalExpressionToArrowExpression(logical_expression);
    }
    default:
      Fail(
          "Unsupported mapping from Skyrise expression to Arrow expression (needs to be either kPqpColumn, "
          "kValue, kPredicate or kLogical).");
  }
}

}  // namespace skyrise
