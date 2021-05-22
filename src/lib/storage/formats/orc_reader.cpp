#include "orc_reader.hpp"

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
  size_t object_size_;
  ObjectReaderStream stream_;
  std::string name_;
};

OrcInputProxy::OrcInputProxy(std::unique_ptr<ObjectReader> source)
    : object_size_(source->GetStatus().GetError() ? 0 : source->GetStatus().GetSize()),
      stream_(std::move(source)),
      name_("OrcInputProxy") {}

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
std::shared_ptr<AbstractSegment> ColumnVectorBatchToSegment(orc::ColumnVectorBatch* column_vector_batch) {
  auto* specialized_batch = dynamic_cast<ColumnVectorBatchType*>(column_vector_batch);
  Assert(specialized_batch != nullptr, "Batch type must match type information.");

  auto result = std::make_shared<ValueSegment<TargetSegmentType>>(false, specialized_batch->numElements);
  auto& destination = result->Values();
  auto* source = specialized_batch->data.data();
  for (size_t i = 0; i < specialized_batch->numElements; i++) {
    destination.push_back(static_cast<TargetSegmentType>(source[i]));
  }
  return result;
}

template <>
std::shared_ptr<AbstractSegment> ColumnVectorBatchToSegment<orc::StringVectorBatch, std::string>(
    orc::ColumnVectorBatch* column_vector_batch) {
  auto* specialized_batch = dynamic_cast<orc::StringVectorBatch*>(column_vector_batch);
  Assert(specialized_batch != nullptr, "Batch type must match type information.");

  auto result = std::make_shared<ValueSegment<std::string>>(false, specialized_batch->numElements);
  auto& destination = result->Values();
  auto* source_data = specialized_batch->data.data();
  auto* source_length = specialized_batch->length.data();
  for (size_t i = 0; i < specialized_batch->numElements; i++) {
    destination.emplace_back(source_data[i], source_length[i]);
  }

  return result;
}

template <>
std::shared_ptr<AbstractSegment> ColumnVectorBatchToSegment<orc::LongVectorBatch, std::string>(
    orc::ColumnVectorBatch* column_vector_batch) {
  auto* specialized_batch = dynamic_cast<orc::LongVectorBatch*>(column_vector_batch);
  Assert(specialized_batch != nullptr, "Batch type must match type information.");

  auto result = std::make_shared<ValueSegment<std::string>>(false, specialized_batch->numElements);
  auto& destination = result->Values();
  auto* source_data = specialized_batch->data.data();
  for (size_t i = 0; i < specialized_batch->numElements; i++) {
    destination.emplace_back(OrcFormatReader::OrcTimestampToDateString(static_cast<int32_t>(source_data[i])));
  }

  return result;
}

std::shared_ptr<AbstractSegment> CreateSegment(orc::ColumnVectorBatch* batch, orc::TypeKind type, bool date_as_string) {
  switch (type) {
    case orc::BOOLEAN:
    case orc::BYTE:
    case orc::INT:
    case orc::SHORT:
      return ColumnVectorBatchToSegment<orc::LongVectorBatch, int32_t>(batch);

    case orc::LONG:
      return ColumnVectorBatchToSegment<orc::LongVectorBatch, int64_t>(batch);

    case orc::FLOAT:
      return ColumnVectorBatchToSegment<orc::DoubleVectorBatch, float>(batch);

    case orc::DOUBLE:
      return ColumnVectorBatchToSegment<orc::DoubleVectorBatch, double>(batch);

    case orc::BINARY:
    case orc::CHAR:
    case orc::VARCHAR:
    case orc::STRING:
      return ColumnVectorBatchToSegment<orc::StringVectorBatch, std::string>(batch);

    case orc::TIMESTAMP:
      return ColumnVectorBatchToSegment<orc::TimestampVectorBatch, int64_t>(batch);

    case orc::DATE:
      return date_as_string ? ColumnVectorBatchToSegment<orc::LongVectorBatch, std::string>(batch)
                            : ColumnVectorBatchToSegment<orc::LongVectorBatch, int64_t>(batch);

    default:
      Fail("Encountered invalid type");
  }
}

}  // namespace

namespace skyrise {

OrcFormatReader::OrcFormatReader(std::unique_ptr<ObjectReader> source, Configuration configuration)
    : configuration_(std::move(configuration)) {
  auto input_stream = std::make_unique<OrcInputProxy>(std::move(source));
  orc::ReaderOptions options;

  try {
    reader_ = orc::createReader(std::move(input_stream), options);

    // TODO(anyone): Once predicates become available through the configuration object and we decided on an internal
    // representation, push them down to orc::Reader.
    orc::RowReaderOptions row_options;
    row_reader_ = reader_->createRowReader(row_options);
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

void OrcFormatReader::ExtractSchema() {
  auto schema = std::make_shared<TableColumnDefinitions>();
  const auto& type = reader_->getType();

  for (size_t i = 0; i < type.getSubtypeCount(); i++) {
    const orc::Type* orc_type = type.getSubtype(i);
    DataType skyrise_type = OrcTypeKindToDataType(orc_type->getKind(), configuration_.parse_dates_as_string);

    // The current ORC definition has no information about whether or not NULL values are allowed for a column.
    // TODO(jansiebert): Implement support for null-values
    const bool nullable = false;

    schema->emplace_back(type.getFieldName(i), skyrise_type, nullable);
  }

  schema_ = std::move(schema);
}

bool OrcFormatReader::HasNext() { return !HasError() && num_rows_read_ < reader_->getNumberOfRows(); }

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

  num_rows_read_ += column_vector_batch_->numElements;
  auto* struct_batch = dynamic_cast<orc::StructVectorBatch*>(column_vector_batch_.get());
  const auto& type = reader_->getType();
  segments.reserve(type.getSubtypeCount());

  for (size_t column_id = 0; column_id < type.getSubtypeCount(); column_id++) {
    segments.emplace_back(CreateSegment(struct_batch->fields[column_id], type.getSubtype(column_id)->getKind(),
                                        configuration_.parse_dates_as_string));
  }

  return std::make_unique<Chunk>(segments);
}

std::string OrcFormatReader::OrcTimestampToDateString(int32_t num_days_since_1970) {
  time_t seconds_since_1970 = static_cast<time_t>(num_days_since_1970) * (60 * 60 * 24);
  tm calendar_date{};
  std::array<char, 11> buffer = {0};  // YYYY-mm-dd + '\0'
  gmtime_r(&seconds_since_1970, &calendar_date);
  strftime(buffer.data(), 11, "%Y-%m-%d", &calendar_date);

  return std::string(buffer.data());
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
