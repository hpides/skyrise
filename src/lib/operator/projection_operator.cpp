#include "projection_operator.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "scheduler/worker/abstract_task.hpp"
#include "scheduler/worker/generic_task.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/assert.hpp"

namespace {

const std::string kName = "Projection";

}  // namespace

namespace skyrise {

ProjectionOperator::ProjectionOperator(std::shared_ptr<const AbstractOperator> input_operator,
                                       const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractOperator(OperatorType::kProjection, std::move(input_operator), nullptr), expressions_(expressions) {}

const std::string& ProjectionOperator::Name() const { return kName; }

std::shared_ptr<const Table> ProjectionOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  std::shared_ptr<const skyrise::Table> input_table = LeftInputTable();
  auto chunk_count = input_table->ChunkCount();

  // TODO(anyone): Implement that properly and ensure that we do not feed large chunks to the expression evaluator for
  // the sake of performance. Also find a proper target chunk size.
  const size_t desired_chunk_size = 128;
  if (chunk_count == 1 && input_table->RowCount() > desired_chunk_size) {
    std::vector<std::shared_ptr<Chunk>> chunks;
    size_t offset = 0;
    while (offset < input_table->RowCount()) {
      std::vector<std::shared_ptr<AbstractSegment>> segments;
      for (size_t a = 0; a < input_table->GetColumnCount(); ++a) {
        auto segment = input_table->GetChunk(0)->GetSegment(a);
        // NOLINTNEXTLINE(performance-unnecessary-value-param)
        ResolveDataType(segment->GetDataType(), [&](auto data_type) {
          using ColumnDataType = decltype(data_type);
          auto casted = dynamic_cast<ValueSegment<ColumnDataType>*>(segment.get());
          auto new_segment = std::make_shared<ValueSegment<ColumnDataType>>();
          for (size_t i = offset; (i < offset + desired_chunk_size && i < input_table->RowCount()); ++i) {
            new_segment->Append(casted->Get(i));
          }
          segments.push_back(std::move(new_segment));
        });
      }
      chunks.emplace_back(std::make_shared<Chunk>(segments));
      offset += desired_chunk_size;
    }
    input_table = std::make_shared<Table>(input_table->ColumnDefinitions(), std::move(chunks));
    chunk_count = input_table->ChunkCount();
  }

  std::vector<Segments> output_segments_by_chunk(chunk_count);
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(chunk_count);

  const size_t expression_count = expressions_.size();

  std::vector<std::atomic_bool> nullable_columns(expression_count);

  bool all_columns_forwarded = true;
  for (const auto& expression : expressions_) {
    if (expression->GetExpressionType() != ExpressionType::kPqpColumn) {
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
      if (expression->GetExpressionType() == ExpressionType::kPqpColumn) {
        const auto& pqp_column_expression = dynamic_cast<const PqpColumnExpression&>(*expression);
        output_segments[column_id] = input_chunk->GetSegment(pqp_column_expression.GetColumnId());
        nullable_columns[column_id] = input_table->ColumnIsNullable(pqp_column_expression.GetColumnId());
      }
    }

    output_segments_by_chunk[chunk_id] = std::move(output_segments);

    if (all_columns_forwarded) {
      continue;
    }

    auto perform_projection_evaluation = [this, chunk_id, expression_count, &output_segments_by_chunk,
                                          &nullable_columns, &input_table]() {
      ExpressionEvaluator evaluator(input_table, chunk_id);

      for (ColumnId column_id = 0; column_id < expression_count; ++column_id) {
        const auto& expression = expressions_[column_id];

        if (expression->GetExpressionType() != ExpressionType::kPqpColumn) {
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
