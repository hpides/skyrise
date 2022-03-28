#include "projection_operator.hpp"

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "scheduler/worker/abstract_task.hpp"
#include "scheduler/worker/generic_task.hpp"
#include "utils/assert.hpp"

namespace {

static const std::string kName = "Projection";

}  // namespace

namespace skyrise {

ProjectionOperator::ProjectionOperator(std::shared_ptr<const AbstractOperator> input_operator,
                                       const std::vector<std::shared_ptr<AbstractExpression>>& init_expressions)
    : AbstractOperator(OperatorType::kProjection, std::move(input_operator), nullptr), expressions_(init_expressions) {}

const std::string& ProjectionOperator::Name() const { return kName; }

std::shared_ptr<const Table> ProjectionOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  const std::shared_ptr<const skyrise::Table> input_table = LeftInputTable();
  const auto chunk_count = input_table->ChunkCount();

  std::vector<Segments> output_segments_by_chunk(chunk_count);
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(chunk_count);

  const size_t expression_count = expressions_.size();

  std::vector<std::atomic_bool> nullable_columns(expression_count);

  bool all_columns_forwarded = true;
  for (const auto& expression : expressions_) {
    if (expression->type_ != ExpressionType::kPqpColumn) {
      all_columns_forwarded = false;
      break;
    }
  }

  for (ChunkId chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
    const auto input_chunk = input_table->GetChunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point.");

    Segments output_segments(expression_count);

    // Execute all projections that only forward an input column in sequential order.
    for (ColumnId column_id = 0; column_id < expression_count; ++column_id) {
      const auto& expression = expressions_[column_id];
      if (expression->type_ == ExpressionType::kPqpColumn) {
        const auto& pqp_column_expression = static_cast<const PqpColumnExpression&>(*expression);
        output_segments[column_id] = input_chunk->GetSegment(pqp_column_expression.column_id_);
        nullable_columns[column_id] = input_table->ColumnIsNullable(pqp_column_expression.column_id_);
      }
    }

    output_segments_by_chunk[chunk_id] = std::move(output_segments);

    if (all_columns_forwarded) {
      continue;
    }

    auto perform_projection_evaluation = [this, chunk_id, expression_count, &output_segments_by_chunk,
                                          &nullable_columns]() {
      ExpressionEvaluator evaluator(LeftInputTable(), chunk_id);

      for (ColumnId column_id = 0; column_id < expression_count; ++column_id) {
        const auto& expression = expressions_[column_id];

        if (expression->type_ != ExpressionType::kPqpColumn) {
          auto output_segment = evaluator.EvaluateExpressionToSegment(*expression);
          nullable_columns[column_id] = nullable_columns[column_id] || output_segment->IsNullable();
          output_segments_by_chunk[chunk_id][column_id] = std::move(output_segment);
        }
      }
    };

    jobs.push_back(std::make_shared<GenericTask>(perform_projection_evaluation));
  }

  operator_execution_context->GetScheduler()->ScheduleAndWaitForTasks(jobs);

  TableColumnDefinitions output_column_definitions;
  output_column_definitions.reserve(expression_count);
  for (ColumnId column_id = 0; column_id < expression_count; ++column_id) {
    const std::shared_ptr<AbstractExpression>& expression = expressions_[column_id];

    output_column_definitions.emplace_back(expression->AsColumnName(), expression->GetDataType(),
                                           nullable_columns[column_id]);
  }

  std::vector<std::shared_ptr<Chunk>> output_chunks(chunk_count);

  for (ChunkId chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
    const auto input_chunk = input_table->GetChunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point.");

    output_chunks[chunk_id] = std::make_shared<Chunk>(std::move(output_segments_by_chunk[chunk_id]));
  }

  return std::make_shared<Table>(output_column_definitions, std::move(output_chunks));
}

}  // namespace skyrise
