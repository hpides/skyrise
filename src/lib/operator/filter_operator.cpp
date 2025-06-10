#include "filter_operator.hpp"

#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "expression/expression_utils.hpp"
#include "filter/expression_evaluator_filter_implementation.hpp"
#include "scheduler/worker/abstract_task.hpp"
#include "scheduler/worker/generic_task.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"

namespace {

const std::string kName = "FilterOperator";

}  // namespace

namespace skyrise {

FilterOperator::FilterOperator(std::shared_ptr<const AbstractOperator> input_operator,
                               std::shared_ptr<AbstractExpression> predicate)
    : AbstractOperator(OperatorType::kFilter, std::move(input_operator), nullptr), predicate_(std::move(predicate)) {}

const std::shared_ptr<AbstractExpression>& FilterOperator::Predicate() const { return predicate_; }

const std::string& FilterOperator::Name() const { return kName; }

std::string FilterOperator::Description(DescriptionMode description_mode) const {
  const auto* const separator = description_mode == DescriptionMode::kMultiLine ? "\n" : " ";

  std::stringstream stream;

  stream << AbstractOperator::Description(description_mode) << separator
         << "Implementation: " << implementation_description_ << separator << predicate_->AsColumnName();

  return stream.str();
}

std::shared_ptr<const Table> FilterOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  const auto& input_table = LeftInputTable();
  implementation_ = CreateImplementation();
  implementation_description_ = implementation_->Description();

  std::mutex output_mutex;

  const ChunkId chunk_count = input_table->ChunkCount();
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(chunk_count);

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(chunk_count);

  for (ChunkId chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
    auto perform_filter = [this, chunk_id, &input_table, &output_mutex, &output_chunks]() {
      const auto& input_chunk = input_table->GetChunk(chunk_id);
      const std::shared_ptr<RowIdPositionList> matches = implementation_->FilterChunk(chunk_id);

      const ColumnCount column_count = input_table->GetColumnCount();
      Segments output_segments;
      output_segments.reserve(column_count);

      if (matches->empty()) {
        return;
      } else if (matches->size() == input_chunk->Size()) {
        // Shortcut if all rows match.
        for (ColumnId column_id = 0; column_id < column_count; ++column_id) {
          const auto& input_segment = input_chunk->GetSegment(column_id);
          output_segments.push_back(input_segment);
        }
      } else {
        // Process matching rows.
        for (ColumnId column_id = 0; column_id < column_count; ++column_id) {
          // NOLINTNEXTLINE(performance-unnecessary-value-param)
          ResolveDataType(input_table->ColumnDataType(column_id), [&](const auto resolved_data_type) {
            using DataType = std::decay_t<decltype(resolved_data_type)>;

            std::vector<DataType> segment_values;
            segment_values.reserve(matches->size());

            const auto input_segment = input_chunk->GetSegment(column_id);

            for (const RowId& match : *matches) {
              segment_values.push_back(std::get<DataType>((*input_segment)[match.chunk_offset]));
            }

            output_segments.push_back(std::make_shared<ValueSegment<DataType>>(std::move(segment_values)));
          });
        }
      }

      const auto chunk = std::make_shared<Chunk>(std::move(output_segments));

      const std::lock_guard<std::mutex> lock(output_mutex);
      output_chunks.push_back(chunk);
    };

    jobs.push_back(std::make_shared<GenericTask>(perform_filter));
  }

  operator_execution_context->GetScheduler()->ScheduleAndWaitForTasks(jobs);

  return std::make_shared<Table>(input_table->ColumnDefinitions(), std::move(output_chunks));
}

std::unique_ptr<AbstractFilterImplementation> FilterOperator::CreateImplementation() {
  // TODO(tobodner): Add further filter implementations and a decision tree.
  return std::make_unique<ExpressionEvaluatorFilterImplementation>(LeftInputTable(), predicate_);
}

void FilterOperator::OnCleanup() { implementation_.reset(); }

}  // namespace skyrise
