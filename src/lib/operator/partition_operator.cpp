#include "partition_operator.hpp"

#include <boost/container_hash/hash.hpp>

#include "resolve_type.hpp"
#include "storage/table/base_value_segment.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/assert.hpp"

namespace skyrise {

static const std::string kName{"Partition"};

PartitionOperator::PartitionOperator(std::shared_ptr<AbstractOperator> input, const size_t partition_count,
                                     const std::set<ColumnId>& partition_column_ids)
    : AbstractOperator(OperatorType::kPartition, std::move(input), nullptr),
      partition_count_(partition_count),
      partition_column_ids_(partition_column_ids) {}

const std::string& PartitionOperator::Name() const { return kName; }

std::shared_ptr<const Table> PartitionOperator::OnExecute() {
  Assert(LeftInput(), "Input operator must not be nullptr.");
  Assert(LeftInputTable(), "Input table must not be nullptr.");

  const auto input_table = LeftInputTable();
  const ChunkOffset chunk_count = input_table->ChunkCount();
  const ColumnCount column_count = input_table->GetColumnCount();

  const PartitionedPositionLists position_lists = GeneratePartitionedPositionLists();

  // Materialize partitions in a new table with one chunk per partition
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(partition_count_);

  for (const auto& position_list : position_lists) {
    Segments segments(column_count);

    // Materialize column by column from original table
    for (ColumnCount column_id = 0; column_id < column_count; column_id++) {
      ResolveType(input_table->ColumnDataType(column_id), [&](auto data_type) {
        using ColumnDataType = decltype(data_type);

        std::vector<std::vector<ColumnDataType>*> input_segments;
        input_segments.reserve(chunk_count);

        for (size_t i = 0; i < chunk_count; i++) {
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

PartitionedPositionLists PartitionOperator::GeneratePartitionedPositionLists() const {
  const auto input_table = LeftInputTable();
  const ChunkId chunk_count = input_table->ChunkCount();
  // NOLINT(hicpp-signed-bitwise)
  // Build hash vector
  std::vector<size_t> hashes(input_table->RowCount());

  for (const auto& partition_column_id : partition_column_ids_) {
    Assert(partition_column_id < input_table->GetColumnCount(), "Column to partition is out of range.");
    Assert(!input_table->ColumnDefinitions()[partition_column_id].nullable, "Nullable columns are not supported.");

    ResolveType(input_table->ColumnDataType(partition_column_id), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      size_t row_index = 0;

      for (ChunkId i = 0; i < chunk_count; i++) {
        const auto abstract_segment = input_table->GetChunk(i)->GetSegment(partition_column_id);
        const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        const auto& segment_values = typed_segment->Values();

        for (const auto& segment_value : segment_values) {
          boost::hash_combine(hashes[row_index++], segment_value);
        }
      }
    });
  }

  // Transform hashes into one position list per partition
  PartitionedPositionLists position_lists(partition_count_);
  ChunkId chunk_index = 0;
  // The row index that is related to the current chunk
  size_t relative_row_index = 0;

  for (const auto& hash : hashes) {
    position_lists[hash % partition_count_].emplace_back(chunk_index, relative_row_index);

    if (relative_row_index == input_table->GetChunk(chunk_index)->Size() - 1) {
      relative_row_index = 0;
      chunk_index++;
    } else {
      relative_row_index++;
    }
  }

  return position_lists;
}

}  // namespace skyrise
