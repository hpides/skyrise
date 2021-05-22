#include "statistics/manifest_writer.hpp"

namespace skyrise {

ManifestWriter::ManifestWriter(std::unique_ptr<ObjectWriter> writer)
    : writer_(std::move(writer)), error_(StorageError::Success()) {}

bool ManifestWriter::WritePartition(const ObjectStatistics& statistics) {
  if (!partition_schema_) {
    partition_schema_ = statistics.schema;
  } else if (*partition_schema_ != *statistics.schema) {
    return false;
  }

  return WritePartitionToStorage(statistics);
}

std::string ManifestWriter::GetManifestVersion() { return std::to_string(kManifestVersion); }

void ManifestWriter::SetSchema(std::shared_ptr<TableColumnDefinitions> partition_schema) {
  partition_schema_ = std::move(partition_schema);
}

void ManifestWriter::SetTablePrefix(std::string table_prefix) { table_prefix_ = std::move(table_prefix); }

std::shared_ptr<BaseValueSegment> ManifestWriter::CreateSegmentForDataType(DataType type, bool nullable) {
  switch (type) {
    case DataType::kString:
      return std::make_shared<ValueSegment<std::string>>(nullable, kMaxCapacity);
    case DataType::kLong:
      return std::make_shared<ValueSegment<int64_t>>(nullable, kMaxCapacity);
    case DataType::kInt:
      return std::make_shared<ValueSegment<int32_t>>(nullable, kMaxCapacity);
    case DataType::kFloat:
      return std::make_shared<ValueSegment<float>>(nullable, kMaxCapacity);
    case DataType::kDouble:
      return std::make_shared<ValueSegment<double>>(nullable, kMaxCapacity);
    default:
      Fail("There is no segment for this type.");
  }
}

TableColumnDefinitions ManifestWriter::GetManifestSchema() {
  TableColumnDefinitions manifest_schema;

  manifest_schema.emplace_back("object_reference", DataType::kString, false);
  manifest_schema.emplace_back("object_format", DataType::kString, false);
  manifest_schema.emplace_back("etag", DataType::kString, false);
  manifest_schema.emplace_back("timestamp", DataType::kLong, false);
  manifest_schema.emplace_back("size", DataType::kLong, false);
  manifest_schema.emplace_back("records", DataType::kLong, false);

  for (const TableColumnDefinition& column : *partition_schema_) {
    manifest_schema.emplace_back(column.name + "_min", column.data_type, true);
    manifest_schema.emplace_back(column.name + "_max", column.data_type, true);
    manifest_schema.emplace_back(column.name + "_nullcount", DataType::kLong, false);
  }

  return manifest_schema;
}

void ManifestWriter::InitManifestSegments() {
  current_segments_.clear();

  current_segments_.push_back(CreateSegmentForDataType(DataType::kString));  // object_reference
  current_segments_.push_back(CreateSegmentForDataType(DataType::kString));  // object_format
  current_segments_.push_back(CreateSegmentForDataType(DataType::kString));  // etag
  current_segments_.push_back(CreateSegmentForDataType(DataType::kLong));    // timestamp
  current_segments_.push_back(CreateSegmentForDataType(DataType::kLong));    // size
  current_segments_.push_back(CreateSegmentForDataType(DataType::kLong));    // records
  for (const TableColumnDefinition& column : *partition_schema_) {
    current_segments_.emplace_back(CreateSegmentForDataType(column.data_type, true));  // min
    current_segments_.emplace_back(CreateSegmentForDataType(column.data_type, true));  // max
    current_segments_.push_back(CreateSegmentForDataType(DataType::kLong));            // nullcount
  }
}

void ManifestWriter::AssertOutputStream() {
  if (formatter_) {
    return;
  }

  OrcFormatterOptions options;
  formatter_ = std::make_unique<OrcFormatter>(options);
  formatter_->SetOutputHandler([this](const char* data, size_t length) {
    if (!error_) {
      error_ = this->writer_->Write(data, length);
    }
  });
  formatter_->Initialize(GetManifestSchema());
  formatter_->AddMetadata("columns", std::to_string(partition_schema_->size()));
  formatter_->AddMetadata("version", std::to_string(kManifestVersion));

  auto buffer = std::make_shared<std::stringstream>();
  BinarySerializationStream serializer(buffer);
  serializer << *partition_schema_;
  formatter_->AddMetadata("schema", buffer->str());
  formatter_->AddMetadata("prefix", table_prefix_);

  InitManifestSegments();
}

void ManifestWriter::Flush() {
  Segments segments;
  for (auto& current_segment : current_segments_) {
    segments.emplace_back(std::move(current_segment));
  }

  formatter_->ProcessChunk(Chunk(segments));
  InitManifestSegments();
}

bool ManifestWriter::WritePartitionToStorage(const ObjectStatistics& statistics) {
  AssertOutputStream();

  if (current_segments_[0]->Size() >= kMaxCapacity) {
    Flush();
  }
  current_segments_[0]->Append(statistics.object_identifier);
  current_segments_[1]->Append(statistics.format);
  current_segments_[2]->Append(statistics.etag);
  current_segments_[3]->Append(statistics.last_modified);
  current_segments_[4]->Append(static_cast<int64_t>(statistics.filesize));
  current_segments_[5]->Append(static_cast<int64_t>(statistics.num_rows));
  for (size_t i = 0; i < statistics.schema->size(); i++) {
    const size_t start_index = 6 + (3 * i);
    current_segments_[start_index]->Append(statistics.minmax[i].first);
    current_segments_[start_index + 1]->Append(statistics.minmax[i].second);
    current_segments_[start_index + 2]->Append(static_cast<int64_t>(statistics.null_count[i]));
  }

  return !error_.IsError();
}

bool ManifestWriter::Close() {
  AssertOutputStream();
  Flush();
  formatter_->Finalize();
  if (error_.IsError()) {
    return error_.IsError();
  }

  return !writer_->Close().IsError();
}

StorageError ManifestWriter::GetError() { return error_; }

}  // namespace skyrise
