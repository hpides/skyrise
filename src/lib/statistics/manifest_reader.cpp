#include "manifest_reader.hpp"

namespace skyrise {

ManifestReader::ManifestReader(const std::shared_ptr<Storage>& storage, const std::string& object_identifier) {
  auto input_stream = std::make_unique<detail::OrcInputStream>(storage, object_identifier);
  orc::ReaderOptions options;

  reader_ = orc::createReader(std::move(input_stream), options);

  schema_ = std::make_shared<TableColumnDefinitions>();
  auto input_buffer = std::make_shared<std::stringstream>(reader_->getMetadataValue("schema"));
  BinarySerializationStream deserializer(input_buffer);
  deserializer >> *schema_;

  number_of_partitions_ = reader_->getNumberOfRows();
  row_reader_ = reader_->createRowReader(row_reader_options_);
  reader_batch_ = row_reader_->createRowBatch(std::min(number_of_partitions_, 256LU));
  current_batch_ = dynamic_cast<orc::StructVectorBatch*>(reader_batch_.get());

  Assert(current_batch_ != nullptr, "Found invalid schema");
}

size_t ManifestReader::GetNumberOfPartitions() const { return number_of_partitions_; }

std::string ManifestReader::GetTablePrefix() { return reader_->getMetadataValue("prefix"); }

std::string ManifestReader::GetManifestVersion() { return reader_->getMetadataValue("version"); }

std::shared_ptr<TableColumnDefinitions> ManifestReader::GetOriginalSchema() { return schema_; }

bool ManifestReader::HasNextPartition() const { return current_partition_index_ < number_of_partitions_; }

template <typename BatchType, typename SkyriseType>
static std::pair<AllTypeVariant, AllTypeVariant> ExtractMinMaxValues(orc::ColumnVectorBatch* min_batch,
                                                                     orc::ColumnVectorBatch* max_batch,
                                                                     size_t row_index) {
  auto* column_min_value = dynamic_cast<BatchType*>(min_batch);
  auto* column_max_value = dynamic_cast<BatchType*>(max_batch);
  AllTypeVariant min_value = static_cast<SkyriseType>(column_min_value->data[row_index]);
  AllTypeVariant max_value = static_cast<SkyriseType>(column_max_value->data[row_index]);
  return std::make_pair(min_value, max_value);
}

void ManifestReader::ReconstructStatistics() {
  parsed_statistics_.clear();

  // First, handle all dynamic casts that do not need to happen for every individual row.
  auto* identifier_column = dynamic_cast<orc::StringVectorBatch*>(current_batch_->fields[0]);
  auto* format_column = dynamic_cast<orc::StringVectorBatch*>(current_batch_->fields[1]);
  auto* etag_column = dynamic_cast<orc::StringVectorBatch*>(current_batch_->fields[2]);
  auto* timestamp_column = dynamic_cast<orc::LongVectorBatch*>(current_batch_->fields[3]);
  auto* size_column = dynamic_cast<orc::LongVectorBatch*>(current_batch_->fields[4]);
  auto* records_column = dynamic_cast<orc::LongVectorBatch*>(current_batch_->fields[5]);

  // We can pre-cast the null-count column for every column in the schema since all columns of this kind have the same
  // fixed type (long). This is not possible with MinMax since these statistics depends on the type of column (long,
  // string, ...) they relates to.
  std::vector<orc::LongVectorBatch*> null_value_counts;
  for (size_t i = 0; i < GetOriginalSchema()->size(); i++) {
    const size_t start_index = 6 + (3 * i);
    null_value_counts.push_back(dynamic_cast<orc::LongVectorBatch*>(current_batch_->fields[start_index + 2]));
  }

  // Create new object statistics and fill with non-dynamic statistics.
  for (size_t row_index = 0; row_index < current_batch_->numElements; row_index++) {
    ObjectStatistics statistics;
    statistics.object_identifier =
        std::string(identifier_column->data[row_index], identifier_column->length[row_index]);
    statistics.format = std::string(format_column->data[row_index], format_column->length[row_index]);
    statistics.etag = std::string(etag_column->data[row_index], etag_column->length[row_index]);
    statistics.last_modified = timestamp_column->data[row_index];
    statistics.filesize = size_column->data[row_index];
    statistics.num_rows = records_column->data[row_index];
    statistics.null_count.reserve(schema_->size());
    statistics.schema = GetOriginalSchema();

    for (size_t schema_index = 0; schema_index < statistics.schema->size(); schema_index++) {
      const size_t start_index = 6 + (3 * schema_index);
      statistics.null_count.push_back(null_value_counts[schema_index]->data[row_index]);

      if (statistics.schema->at(schema_index).data_type == DataType::kLong) {
        statistics.minmax.emplace_back(ExtractMinMaxValues<orc::LongVectorBatch, int64_t>(
            current_batch_->fields[start_index], current_batch_->fields[start_index + 1], row_index));
      } else if (statistics.schema->at(schema_index).data_type == DataType::kInt) {
        statistics.minmax.emplace_back(ExtractMinMaxValues<orc::LongVectorBatch, int32_t>(
            current_batch_->fields[start_index], current_batch_->fields[start_index + 1], row_index));
      } else if (statistics.schema->at(schema_index).data_type == DataType::kFloat) {
        statistics.minmax.emplace_back(ExtractMinMaxValues<orc::DoubleVectorBatch, float>(
            current_batch_->fields[start_index], current_batch_->fields[start_index + 1], row_index));
      } else if (statistics.schema->at(schema_index).data_type == DataType::kDouble) {
        statistics.minmax.emplace_back(ExtractMinMaxValues<orc::DoubleVectorBatch, double>(
            current_batch_->fields[start_index], current_batch_->fields[start_index + 1], row_index));
      } else if (statistics.schema->at(schema_index).data_type == DataType::kString) {
        auto* column_min_value = dynamic_cast<orc::StringVectorBatch*>(current_batch_->fields[start_index]);
        auto* column_max_value = dynamic_cast<orc::StringVectorBatch*>(current_batch_->fields[start_index + 1]);
        AllTypeVariant min_value = std::string(column_min_value->data[row_index], column_min_value->length[row_index]);
        AllTypeVariant max_value = std::string(column_max_value->data[row_index], column_max_value->length[row_index]);
        statistics.minmax.emplace_back(std::make_pair(min_value, max_value));
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
  if (current_partition_index_ == 0 || current_batch_index_ >= reader_batch_->numElements) {
    row_reader_->next(*reader_batch_);
    current_batch_index_ = 0;
    Assert(reader_batch_->numElements > 0, "Read empty batch");
    ReconstructStatistics();
  }

  current_partition_index_++;
  return parsed_statistics_[current_batch_index_++];
}

}  // namespace skyrise
