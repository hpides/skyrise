#include "manifest_reader.hpp"

#include "serialization/schema_serialization.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/assert.hpp"

namespace skyrise {

ManifestReader::ManifestReader(std::unique_ptr<ObjectReader> source) : OrcFormatReader(std::move(source)) {
  ReconstructStatisticsFromChunk(Next());
}

size_t ManifestReader::GetNumberOfPartitions() const { return reader_->getNumberOfRows(); }

std::string ManifestReader::GetTablePrefix() { return reader_->getMetadataValue("prefix"); }

std::string ManifestReader::GetManifestVersion() { return reader_->getMetadataValue("version"); }

std::shared_ptr<const TableColumnDefinitions> ManifestReader::GetOriginalSchema() {
  if (!partition_schema_) {
    partition_schema_ = std::make_shared<TableColumnDefinitions>();
    auto input_buffer = std::make_shared<std::stringstream>(reader_->getMetadataValue("schema"));
    BinarySerializationStream deserializer(input_buffer);
    deserializer >> *partition_schema_;
  }

  return partition_schema_;
}

bool ManifestReader::HasNextPartition() { return current_partition_index_ < parsed_statistics_.size() || HasNext(); }

template <typename SkyriseType>
static std::pair<SkyriseType, SkyriseType> ExtractMinMaxValues(AbstractSegment* min_batch, AbstractSegment* max_batch,
                                                               uint32_t row_index) {
  auto* column_min_value = dynamic_cast<ValueSegment<SkyriseType>*>(min_batch);
  auto* column_max_value = dynamic_cast<ValueSegment<SkyriseType>*>(max_batch);
  SkyriseType min_value = column_min_value->get(row_index);
  SkyriseType max_value = column_max_value->get(row_index);
  return std::make_pair(min_value, max_value);
}

void ManifestReader::ReconstructStatisticsFromChunk(std::unique_ptr<Chunk> chunk) {
  parsed_statistics_.clear();

  if (chunk == nullptr) {
    return;
  }

  // First, handle all dynamic casts that do not need to happen for every individual row.
  auto* identifier_column = dynamic_cast<ValueSegment<std::string>*>(chunk->GetSegment(0).get());
  auto* format_column = dynamic_cast<ValueSegment<std::string>*>(chunk->GetSegment(1).get());
  auto* etag_column = dynamic_cast<ValueSegment<std::string>*>(chunk->GetSegment(2).get());
  auto* timestamp_column = dynamic_cast<ValueSegment<int64_t>*>(chunk->GetSegment(3).get());
  auto* size_column = dynamic_cast<ValueSegment<int64_t>*>(chunk->GetSegment(4).get());
  auto* records_column = dynamic_cast<ValueSegment<int64_t>*>(chunk->GetSegment(5).get());

  // We can pre-cast the null-count column for every column in the schema since all columns of this kind have the same
  // fixed type (long). This is not possible with MinMax since these statistics depends on the type of column (long,
  // string, ...) they relates to.
  std::vector<ValueSegment<int64_t>*> null_value_counts;
  for (size_t i = 0; i < GetOriginalSchema()->size(); ++i) {
    // The null value statistic is the third one (index 2) for each column of the original schema after min and max.
    const size_t null_value_index = 6 + (3 * i) + 2;
    null_value_counts.push_back(dynamic_cast<ValueSegment<int64_t>*>(chunk->GetSegment(null_value_index).get()));
  }

  // Create new object statistics and fill with non-dynamic statistics.
  for (size_t row_index = 0; row_index < chunk->Size(); ++row_index) {
    ObjectStatistics statistics;
    statistics.object_identifier = identifier_column->get(row_index);
    statistics.format = format_column->get(row_index);
    statistics.etag = etag_column->get(row_index);
    statistics.last_modified = timestamp_column->get(row_index);
    statistics.filesize = size_column->get(row_index);
    statistics.num_rows = records_column->get(row_index);
    statistics.null_count.reserve(schema_->size());
    statistics.schema = GetOriginalSchema();

    for (size_t schema_index = 0; schema_index < statistics.schema->size(); ++schema_index) {
      const size_t start_index = 6 + (3 * schema_index);
      statistics.null_count.push_back(null_value_counts[schema_index]->get(row_index));

      auto* min_column = chunk->GetSegment(start_index).get();
      auto* max_column = chunk->GetSegment(start_index + 1).get();

      if (max_column->GetDataType() == DataType::kLong) {
        statistics.minmax.emplace_back(ExtractMinMaxValues<int64_t>(min_column, max_column, row_index));
      } else if (max_column->GetDataType() == DataType::kInt) {
        statistics.minmax.emplace_back(ExtractMinMaxValues<int32_t>(min_column, max_column, row_index));
      } else if (max_column->GetDataType() == DataType::kFloat) {
        statistics.minmax.emplace_back(ExtractMinMaxValues<float>(min_column, max_column, row_index));
      } else if (max_column->GetDataType() == DataType::kDouble) {
        statistics.minmax.emplace_back(ExtractMinMaxValues<double>(min_column, max_column, row_index));
      } else if (max_column->GetDataType() == DataType::kString) {
        auto* column_min_value = dynamic_cast<ValueSegment<std::string>*>(min_column);
        auto* column_max_value = dynamic_cast<ValueSegment<std::string>*>(max_column);
        const AllTypeVariant min_value = column_min_value->get(row_index);
        const AllTypeVariant max_value = column_max_value->get(row_index);
        statistics.minmax.emplace_back(min_value, max_value);
      } else {
        Fail("Type not supported.");
      }
    }

    parsed_statistics_.emplace_back(std::move(statistics));
  }
}

ObjectStatistics ManifestReader::ReadNextPartition() {
  Assert(HasNextPartition(), "HasNextPartition() has to be true");

  // Fill batch with new data.
  if (current_partition_index_ >= parsed_statistics_.size()) {
    current_partition_index_ = 0;
    ReconstructStatisticsFromChunk(Next());
  }

  return parsed_statistics_[current_partition_index_++];
}

}  // namespace skyrise
