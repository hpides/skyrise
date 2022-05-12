#include "orc_reader.hpp"

#include <magic_enum.hpp>

#include "serialization/binary_serialization_stream.hpp"
#include "storage/backend/stream.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/literal.hpp"

namespace {

using namespace skyrise;  // NOLINT(google-build-using-namespace)

class OrcInputProxy : public orc::InputStream {
 public:
  explicit OrcInputProxy(std::unique_ptr<ObjectReader> source);

  uint64_t getLength() const override;
  uint64_t getNaturalReadSize() const override;
  void read(void* buf, uint64_t length, uint64_t offset) override;
  const std::string& getName() const override;

 private:
  static constexpr size_t kNaturalReadSize = 20_MB;
  ObjectReaderStream stream_;
  size_t object_size_;
  std::string name_;
};

OrcInputProxy::OrcInputProxy(std::unique_ptr<ObjectReader> source)
    : stream_(std::move(source), true), name_("OrcInputProxy") {
  // Get Size of object from stream.
  stream_.seekg(0, std::ios::end);
  object_size_ = stream_.tellg();
  stream_.seekg(0, std::ios::beg);
}

uint64_t OrcInputProxy::getLength() const { return object_size_; }

uint64_t OrcInputProxy::getNaturalReadSize() const { return kNaturalReadSize; }

void OrcInputProxy::read(void* buf, uint64_t length, uint64_t offset) {
  stream_.seekg(static_cast<std::streamoff>(offset));
  stream_.read(static_cast<char*>(buf), length);

  if (!stream_.good() || stream_.gcount() != static_cast<std::streamsize>(length)) {
    throw std::logic_error("Error while reading from orc file.");
  }
}

const std::string& OrcInputProxy::getName() const { return name_; }

// Generic and specialized functions to create Skyrise segments from ORC ColumnVectorBatch objects.
template <typename ColumnVectorBatchType, typename TargetSegmentType>
std::shared_ptr<AbstractSegment> ColumnVectorBatchToSegment(orc::ColumnVectorBatch* column_vector_batch,
                                                            size_t length) {
  auto* specialized_batch = dynamic_cast<ColumnVectorBatchType*>(column_vector_batch);
  Assert(specialized_batch != nullptr, "Batch type must match type information.");

  auto result = std::make_shared<ValueSegment<TargetSegmentType>>(false, length);
  auto& destination = result->Values();
  auto* source = specialized_batch->data.data();
  for (size_t i = 0; i < length; ++i) {
    destination.push_back(static_cast<TargetSegmentType>(source[i]));
  }
  return result;
}

template <>
std::shared_ptr<AbstractSegment> ColumnVectorBatchToSegment<orc::StringVectorBatch, std::string>(
    orc::ColumnVectorBatch* column_vector_batch, size_t length) {
  auto* specialized_batch = dynamic_cast<orc::StringVectorBatch*>(column_vector_batch);
  Assert(specialized_batch != nullptr, "Batch type must match type information.");

  auto result = std::make_shared<ValueSegment<std::string>>(false, length);
  auto& destination = result->Values();
  auto* source_data = specialized_batch->data.data();
  auto* source_length = specialized_batch->length.data();
  for (size_t i = 0; i < length; ++i) {
    destination.emplace_back(source_data[i], source_length[i]);
  }

  return result;
}

template <>
std::shared_ptr<AbstractSegment> ColumnVectorBatchToSegment<orc::LongVectorBatch, std::string>(
    orc::ColumnVectorBatch* column_vector_batch, size_t length) {
  auto* specialized_batch = dynamic_cast<orc::LongVectorBatch*>(column_vector_batch);
  Assert(specialized_batch != nullptr, "Batch type must match type information.");

  auto result = std::make_shared<ValueSegment<std::string>>(false, length);
  auto& destination = result->Values();
  auto* source_data = specialized_batch->data.data();
  for (size_t i = 0; i < length; ++i) {
    destination.emplace_back(OrcFormatReader::OrcTimestampToDateString(static_cast<int32_t>(source_data[i])));
  }

  return result;
}

std::shared_ptr<AbstractSegment> CreateSegment(orc::ColumnVectorBatch* batch, orc::TypeKind type, bool date_as_string,
                                               size_t length) {
  switch (type) {
    case orc::BOOLEAN:
    case orc::BYTE:
    case orc::INT:
    case orc::SHORT:
      return ColumnVectorBatchToSegment<orc::LongVectorBatch, int32_t>(batch, length);

    case orc::LONG:
      return ColumnVectorBatchToSegment<orc::LongVectorBatch, int64_t>(batch, length);

    case orc::FLOAT:
      return ColumnVectorBatchToSegment<orc::DoubleVectorBatch, float>(batch, length);

    case orc::DOUBLE:
      return ColumnVectorBatchToSegment<orc::DoubleVectorBatch, double>(batch, length);

    case orc::BINARY:
    case orc::CHAR:
    case orc::VARCHAR:
    case orc::STRING:
      return ColumnVectorBatchToSegment<orc::StringVectorBatch, std::string>(batch, length);

    case orc::TIMESTAMP:
      return ColumnVectorBatchToSegment<orc::TimestampVectorBatch, int64_t>(batch, length);

    case orc::DATE:
      return date_as_string ? ColumnVectorBatchToSegment<orc::LongVectorBatch, std::string>(batch, length)
                            : ColumnVectorBatchToSegment<orc::LongVectorBatch, int64_t>(batch, length);

    default:
      Fail("Encountered invalid type");
  }
}

}  // namespace

namespace skyrise {

OrcFormatReader::OrcFormatReader(std::unique_ptr<ObjectReader> source, Configuration configuration)
    : configuration_(std::move(configuration)) {
  auto input_stream = std::make_unique<OrcInputProxy>(std::move(source));
  Assert(!(configuration_.select_partition_range.has_value() && configuration_.select_row_range.has_value()),
         "You may only select by partition or rows.");
  orc::ReaderOptions options;

  try {
    reader_ = orc::createReader(std::move(input_stream), options);

    // TODO(anyone): Once predicates become available through the configuration object and we decided on an internal
    // representation, push them down to orc::Reader.
    orc::RowReaderOptions row_options;
    row_reader_ = reader_->createRowReader(row_options);

    if (configuration_.select_partition_range.has_value() || configuration_.select_row_range.has_value()) {
      SeekToSelectedRows();
    }

    column_vector_batch_ = row_reader_->createRowBatch(kChunkDefaultSize);

    // Identifies and sets schema_.
    ExtractSchema();

    if (configuration_.expected_schema) {
      if (*schema_ != *configuration_.expected_schema) {
        SetError(StorageError(StorageErrorType::kInvalidArgument, "Unexpected schema found."));
      }
    }

  } catch (const std::logic_error& error) {
    // Handle I/O errors.
    SetError(StorageError(StorageErrorType::kIOError, error.what()));
  } catch (const orc::ParseError& error) {
    // Parsing errors are treated the same.
    SetError(StorageError(StorageErrorType::kIOError, error.what()));
  }
}

std::vector<size_t> OrcFormatReader::ExtractPartitionInformation() {
  std::string payload = reader_->getMetadataValue("partition_offsets");
  Assert(!payload.empty(), "Parition information expected.");

  auto stream = std::make_shared<std::stringstream>(payload);
  BinarySerializationStream deserializer(stream);

  int64_t num_partitions = 0;
  deserializer >> num_partitions;

  // In case of currupted data, we want to ensure that the following reserve() does not allocate all the RAM. We chose
  // 2^14, since this is also related to the maximum number of AWS Lambda functions that we want to run on concurrently.
  Assert(num_partitions >= 0 && num_partitions < 16384,
         "Sanity check failed. Partition payload is probably corrupted.");

  std::vector<size_t> partition_offsets;
  partition_offsets.reserve(num_partitions);

  int64_t deserialized_partition_offset = 0;
  for (int64_t i = 0; i < num_partitions; ++i) {
    deserializer >> deserialized_partition_offset;
    Assert(deserializer.good(), "Partition payload is corrupted.");

    partition_offsets.push_back(static_cast<size_t>(deserialized_partition_offset));
  }

  return partition_offsets;
}

void OrcFormatReader::SeekToSelectedRows() {
  size_t first_row_index = 0;
  size_t last_row_index = 0;

  if (configuration_.select_row_range.has_value()) {
    // Rows ranges a provided explicitly.
    first_row_index = configuration_.select_row_range->first;
    last_row_index = configuration_.select_row_range->second;
  } else {
    // Rows ranges are derived from partition indexes.
    std::vector<size_t> partition_offsets = ExtractPartitionInformation();
    const size_t first_partition_index = configuration_.select_partition_range->first;
    const size_t last_partition_index = configuration_.select_partition_range->second;

    Assert(first_partition_index < partition_offsets.size(), "Partition index out of bounds.");
    Assert(last_partition_index < partition_offsets.size(), "Partition index out of bounds.");
    Assert(first_partition_index <= last_partition_index, "Partition index out of bounds.");

    first_row_index = first_partition_index > 0 ? partition_offsets[first_partition_index - 1] : 0;
    last_row_index = partition_offsets[last_partition_index] - 1;
  }

  if (first_row_index > last_row_index) {
    read_at_most_num_rows_ = 0;
    return;
  }

  if (first_row_index > 0) {
    row_reader_->seekToRow(first_row_index);
  }

  read_at_most_num_rows_ = last_row_index - first_row_index + 1;
}

void OrcFormatReader::ExtractSchema() {
  auto schema = std::make_shared<TableColumnDefinitions>();
  const auto& type = reader_->getType();

  for (size_t i = 0; i < type.getSubtypeCount(); ++i) {
    const orc::Type* orc_type = type.getSubtype(i);
    DataType skyrise_type = OrcTypeKindToDataType(orc_type->getKind(), configuration_.parse_dates_as_string);

    // The current ORC definition has no information about whether or not NULL values are allowed for a column.
    // TODO(jansiebert): Implement support for null-values
    const bool nullable = false;

    schema->emplace_back(type.getFieldName(i), skyrise_type, nullable);
  }

  schema_ = std::move(schema);
}

bool OrcFormatReader::HasNext() {
  return !HasError() && num_rows_read_ < reader_->getNumberOfRows() && num_rows_read_ < read_at_most_num_rows_;
}

std::unique_ptr<Chunk> OrcFormatReader::Next() {
  Segments segments;
  try {
    if (!row_reader_->next(*column_vector_batch_) || column_vector_batch_->numElements == 0) {
      // If we did not read any rows, we return an empty Chunk.
      return nullptr;
    }
  } catch (const std::logic_error& e) {
    SetError(StorageError(StorageErrorType::kIOError, e.what()));
    return nullptr;
  }

  // Make sure to not read over selected row range boundaries.
  size_t num_rows_read_now = column_vector_batch_->numElements;
  if (read_at_most_num_rows_ > 0 && num_rows_read_ + num_rows_read_now > read_at_most_num_rows_) {
    num_rows_read_now = read_at_most_num_rows_ - num_rows_read_;
  }

  num_rows_read_ += num_rows_read_now;
  auto* struct_batch = dynamic_cast<orc::StructVectorBatch*>(column_vector_batch_.get());
  const auto& type = reader_->getType();
  segments.reserve(type.getSubtypeCount());

  for (size_t column_id = 0; column_id < type.getSubtypeCount(); ++column_id) {
    segments.emplace_back(CreateSegment(struct_batch->fields[column_id], type.getSubtype(column_id)->getKind(),
                                        configuration_.parse_dates_as_string, num_rows_read_now));
  }

  return std::make_unique<Chunk>(segments);
}

std::string OrcFormatReader::OrcTimestampToDateString(int32_t num_days_since_1970) {
  time_t seconds_since_1970 = static_cast<time_t>(num_days_since_1970) * static_cast<time_t>(60 * 60 * 24);
  tm calendar_date{};
  std::array<char, 11> buffer = {0};  // YYYY-mm-dd + '\0'
  gmtime_r(&seconds_since_1970, &calendar_date);
  strftime(buffer.data(), 11, "%Y-%m-%d", &calendar_date);

  return {buffer.data()};
}

DataType OrcFormatReader::OrcTypeKindToDataType(orc::TypeKind type, bool date_as_string) {
  switch (type) {
    case orc::FLOAT:
      return DataType::kFloat;

    case orc::DOUBLE:
    case orc::DECIMAL:
      return DataType::kDouble;

    case orc::INT:
    case orc::BYTE:
    case orc::BOOLEAN:
    case orc::SHORT:
      return DataType::kInt;

    case orc::LONG:
    case orc::TIMESTAMP:
      return DataType::kLong;

    case orc::STRING:
    case orc::CHAR:
    case orc::VARCHAR:
    case orc::BINARY:
      return DataType::kString;

    case orc::DATE:
      return date_as_string ? DataType::kString : DataType::kLong;

    default:
      std::stringstream fail_message;
      fail_message << "Encountered unsupported type: " << magic_enum::enum_name(type);
      throw std::logic_error(fail_message.str());  // Use logic_error instead of Fail(...) because we can catch this.
  }
}

}  // namespace skyrise
