/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "expression_evaluator_filter_implementation.hpp"

#include "expression/expression_utils.hpp"

namespace skyrise {

ExpressionEvaluatorFilterImplementation::ExpressionEvaluatorFilterImplementation(
    std::shared_ptr<const Table> input_table, std::shared_ptr<const AbstractExpression> expression)
    : input_table_(std::move(input_table)), expression_(std::move(expression)) {}

std::string ExpressionEvaluatorFilterImplementation::Description() const {
  return "ExpressionEvaluatorFilterImplementation";
}

std::shared_ptr<RowIdPositionList> ExpressionEvaluatorFilterImplementation::FilterChunk(ChunkId chunk_id) {
  return std::make_shared<RowIdPositionList>(
      ExpressionEvaluator(input_table_, chunk_id).EvaluateExpressionToPositionList(*expression_));
}

}  // namespace skyrise
