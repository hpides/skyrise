/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/extract_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression_result.hpp"
#include "operator/abstract_operator.hpp"
#include "storage/storage_types.hpp"
#include "storage/table/base_value_segment.hpp"
#include "storage/table/table.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * Computes the result of an Expression in three different ways.
 *   - EvaluateExpressionToResult(): Result is an ExpressionResult<> (with one entry per row in the input Chunk) or a
 *                                   single row, if no input Chunk is specified.
 *   - EvaluateExpressionToSegment(): Wraps EvaluateExpressionToResult() into a Segment.
 *   - EvaluateExpressionToPositionList(): Only for Expressions returning Bools. Returns a RowIdPositionList of the Rows
 *                                         where the Expression is True. Useful for scans with complex predicates.
 *
 * Operates either on a Chunk (returning a value for each row in it) or without a Chunk (returning a single value and
 * failing if Columns are encountered in the Expression).
 */
class ExpressionEvaluator final {
 public:
  using Bool = int32_t;

  /*
   * For Expressions that do not reference any columns (e.g., in the LIMIT clause).
   */
  ExpressionEvaluator() = default;

  /*
   * For Expressions that reference segments from a single table.
   */
  ExpressionEvaluator(const std::shared_ptr<const Table>& table, const ChunkId chunk_id);

  std::shared_ptr<BaseValueSegment> EvaluateExpressionToSegment(const AbstractExpression& expression);
  RowIdPositionList EvaluateExpressionToPositionList(const AbstractExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateExpressionToResult(const AbstractExpression& expression);

 private:
  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateArithmeticExpression(const ArithmeticExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateBinaryPredicateExpression(
      const BinaryPredicateExpression& expression);

  template <typename Result, typename Functor>
  std::shared_ptr<ExpressionResult<Result>> EvaluateBinaryWithDefaultNullLogic(
      const AbstractExpression& left_expression, const AbstractExpression& right_expression);

  template <typename Result, typename Functor>
  std::shared_ptr<ExpressionResult<Result>> EvaluateBinaryWithFunctorBasedNullLogic(
      const AbstractExpression& left_expression, const AbstractExpression& right_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateCaseExpression(const CaseExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateCastExpression(const CastExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateColumnExpression(const PqpColumnExpression& column_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateExtractExpression(const ExtractExpression& extract_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateInExpression(const InExpression& in_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateIsNullExpression(const IsNullExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateLogicalExpression(const LogicalExpression& expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluatePredicateExpression(
      const AbstractPredicateExpression& predicate_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateUnaryMinusExpression(
      const UnaryMinusExpression& unary_minus_expression);

  template <typename Result>
  std::shared_ptr<ExpressionResult<Result>> EvaluateValueOrCorrelatedParameterExpression(
      const AbstractExpression& expression);

  template <size_t Offset, size_t Count>
  std::shared_ptr<ExpressionResult<std::string>> EvaluateExtractSubstr(
      const ExpressionResult<std::string>& from_result);

  template <typename Functor>
  void ResolveToExpressionResultView(const AbstractExpression& expression, const Functor& functor);

  template <typename Functor>
  void ResolveToExpressionResultViews(const AbstractExpression& left_expression,
                                      const AbstractExpression& right_expression, const Functor& functor);

  template <typename Functor>
  void ResolveToExpressionResult(const AbstractExpression& expression, const Functor& functor);
  template <typename Functor>
  void ResolveToExpressionResults(const AbstractExpression& left_expression, const AbstractExpression& right_expression,
                                  const Functor& functor);

  /**
   * Compute the number of rows that any kind expression produces based on the number of rows in its parameters.
   */
  template <typename... RowCounts>
  static ChunkOffset ResultSize(const RowCounts... row_counts);

  /**
   * "Default null logic" means that if one of the arguments of an operation is NULL, the result is NULL.
   *
   * Either operand can be either empty (the operand is not nullable), contain one element (the operand is a literal
   * with null info), or can have n rows (the operand is a nullable series).
   */
  static std::vector<bool> EvaluateDefaultNullLogic(const std::vector<bool>& left, const std::vector<bool>& right);

  void MaterializeSegmentIfNotYetMaterialized(const ColumnId column_id);

  std::shared_ptr<ExpressionResult<std::string>> EvaluateSubstring(
      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);
  std::shared_ptr<ExpressionResult<std::string>> EvaluateConcatenate(
      const std::vector<std::shared_ptr<AbstractExpression>>& arguments);

  std::shared_ptr<const Table> table_;
  std::shared_ptr<const Chunk> chunk_;

  const ChunkId chunk_id_ = 0;
  size_t output_row_count_ = 1;

  // One entry for each segment in the chunk_; may be nullptr if the segment has not been materialized.
  std::vector<std::shared_ptr<BaseExpressionResult>> segment_materializations_;

  // Some expressions can be reused; either in the same result column (SELECT (a+3)*(a+3)), or across columns.
  ConstExpressionUnorderedMap<std::shared_ptr<BaseExpressionResult>> cached_expression_results_;
};

}  // namespace skyrise
