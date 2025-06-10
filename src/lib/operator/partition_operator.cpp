#include "partition_operator.hpp"

#include <boost/container_hash/hash.hpp>

#include "all_type_variant.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/assert.hpp"

namespace {

const std::string kName = "Partition";

}  // namespace

namespace skyrise {

PartitionOperator::PartitionOperator(std::shared_ptr<AbstractOperator> input,
                                     std::shared_ptr<AbstractPartitioningFunction> partitioning_function)
    : AbstractOperator(OperatorType::kPartition, std::move(input), nullptr),
      partitioning_function_(std::move(partitioning_function)) {}

const std::string& PartitionOperator::Name() const { return kName; }

std::shared_ptr<const Table> PartitionOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& /*operator_execution_context*/) {
  Assert(LeftInput(), "Input operator must not be nullptr.");
  Assert(LeftInputTable(), "Input table must not be nullptr.");

  const auto input_table = LeftInputTable();
  const ChunkOffset chunk_count = input_table->ChunkCount();
  const ColumnCount column_count = input_table->GetColumnCount();

  const PartitionedPositionLists position_lists = partitioning_function_->Partition(input_table);

  // Materialize partitions in a new table with one chunk per partition
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(position_lists.size());

  for (const auto& position_list : position_lists) {
    Segments segments(column_count);

    // Materialize column by column from original table
    for (ColumnCount column_id = 0; column_id < column_count; ++column_id) {
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      ResolveDataType(input_table->ColumnDataType(column_id), [&](auto data_type) {
        using ColumnDataType = decltype(data_type);

        std::vector<std::vector<ColumnDataType>*> input_segments;
        input_segments.reserve(chunk_count);

        for (size_t i = 0; i < chunk_count; ++i) {
          const auto current_chunk = input_table->GetChunk(i);
          const auto abstract_segment = current_chunk->GetSegment(column_id);
          const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
          input_segments.emplace_back(&typed_segment->Values());
        }

        std::vector<ColumnDataType> output_segment_values;
        output_segment_values.reserve(position_list.size());

        for (const auto& [chunk_index, relative_row_index] : position_list) {
          const auto& current_segment_values = *input_segments[chunk_index];
          output_segment_values.emplace_back(current_segment_values[relative_row_index]);
        }

        segments[column_id] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(output_segment_values));
      });
    }

    output_chunks.emplace_back(std::make_shared<Chunk>(segments));
  }

  return std::make_shared<Table>(LeftInputTable()->ColumnDefinitions(), std::move(output_chunks));
}

}  // namespace skyrise
