/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_filter_implementation.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "storage/storage_types.hpp"

namespace skyrise {

/**
 * Default implementation of the AbstractFilterImplementation. This is likely slower than any specialized
 * AbstractFilterImplementation and should, thus, only be used if a particular expression type does not have a
 * specialized AbstractFilterImplementation.
 */
class ExpressionEvaluatorFilterImplementation : public AbstractFilterImplementation {
 public:
  ExpressionEvaluatorFilterImplementation(std::shared_ptr<const Table> input_table,
                                          std::shared_ptr<const AbstractExpression> expression);

  std::string Description() const override;
  std::shared_ptr<RowIdPositionList> FilterChunk(ChunkId chunk_id) override;

 private:
  std::shared_ptr<const Table> input_table_;
  std::shared_ptr<const AbstractExpression> expression_;
};

}  // namespace skyrise
