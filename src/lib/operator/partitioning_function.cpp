#include "partitioning_function.hpp"

#include <boost/container_hash/hash.hpp>
#include <magic_enum/magic_enum.hpp>

#include "all_type_variant.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/assert.hpp"

namespace skyrise {

AbstractPartitioningFunction::AbstractPartitioningFunction(const PartitioningFunctionType type,
                                                           const std::set<ColumnId>& partition_column_ids,
                                                           const size_t partition_count)
    : type_(type), partition_column_ids_(partition_column_ids), partition_count_(partition_count) {}

std::shared_ptr<AbstractPartitioningFunction> AbstractPartitioningFunction::FromJson(
    const Aws::Utils::Json::JsonView& json) {
  const auto type = magic_enum::enum_cast<PartitioningFunctionType>(json.GetString("type")).value();

  if (type == PartitioningFunctionType::kHash) {
    const size_t partition_count = json.GetInteger("partition_count");
    const auto partition_column_ids = json.GetArray("partition_column_ids");

    // Store ColumnIds in a set to provide a deterministic order.
    std::set<ColumnId> sorted_partition_column_ids;
    for (size_t i = 0; i < partition_column_ids.GetLength(); ++i) {
      sorted_partition_column_ids.insert(partition_column_ids[i].AsInteger());
    }

    return std::make_shared<HashPartitioningFunction>(sorted_partition_column_ids, partition_count);
  }

  Fail("PartitioningFunctionType not supported.");
}

PartitioningFunctionType AbstractPartitioningFunction::Type() const { return type_; }

const std::set<ColumnId>& AbstractPartitioningFunction::PartitionColumnIds() const { return partition_column_ids_; }

size_t AbstractPartitioningFunction::PartitionCount() const { return partition_count_; }

HashPartitioningFunction::HashPartitioningFunction(const std::set<ColumnId>& partition_column_ids,
                                                   const size_t partition_count)
    : AbstractPartitioningFunction(PartitioningFunctionType::kHash, partition_column_ids, partition_count) {}

PartitionedPositionLists HashPartitioningFunction::Partition(const std::shared_ptr<const Table>& table) const {
  const ChunkId chunk_count = table->ChunkCount();

  // Build hash vector
  std::vector<size_t> hashes(table->RowCount());

  for (const auto& partition_column_id : partition_column_ids_) {
    Assert(partition_column_id < table->GetColumnCount(), "Column to partition is out of range.");
    Assert(!table->ColumnDefinitions()[partition_column_id].nullable, "Nullable columns are not supported.");

    ResolveDataType(table->ColumnDataType(partition_column_id), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      size_t row_index = 0;

      for (ChunkId i = 0; i < chunk_count; ++i) {
        const auto abstract_segment = table->GetChunk(i)->GetSegment(partition_column_id);
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
  std::vector<size_t> chunk_sizes(chunk_count);
  for (size_t i = 0; i < chunk_count; ++i) {
    chunk_sizes[i] = table->GetChunk(i)->Size();
  }

  for (const auto& hash : hashes) {
    position_lists[hash % partition_count_].emplace_back(chunk_index, relative_row_index);

    if (relative_row_index == chunk_sizes[chunk_index] - 1) {
      relative_row_index = 0;
      ++chunk_index;
    } else {
      ++relative_row_index;
    }
  }

  return position_lists;
}

Aws::Utils::Json::JsonValue HashPartitioningFunction::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> partition_column_id_array(partition_column_ids_.size());

  size_t i = 0;
  for (const auto& partition_column_id : partition_column_ids_) {
    partition_column_id_array[i++] = Aws::Utils::Json::JsonValue().AsInteger(partition_column_id);
  }

  return Aws::Utils::Json::JsonValue()
      .WithString("type", std::string(magic_enum::enum_name(type_)))
      .WithArray("partition_column_ids", partition_column_id_array)
      .WithInteger("partition_count", partition_count_);
}

}  // namespace skyrise
