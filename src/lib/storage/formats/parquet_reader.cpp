#include "parquet_reader.hpp"

#include <algorithm>
#include <numeric>
#include <variant>

#include <arrow/array.h>
#include <arrow/array/array_binary.h>
#include <arrow/buffer_builder.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/io/file.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <magic_enum.hpp>
#include <parquet/stream_reader.h>

#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "orc_reader.hpp"
#include "storage/backend/stream.hpp"
#include "storage/table/value_segment.hpp"

#define HANDLE_RESULT(result, reason) \
  if (!result.ok()) {                 \
    throw std::logic_error(reason);   \
  }

namespace {

class ParquetInputProxy : public arrow::io::RandomAccessFile {
 public:
  ParquetInputProxy(std::unique_ptr<skyrise::ObjectReader> source) : stream_(std::move(source), true) {
    stream_.seekg(0, std::ios::end);
    object_size_ = stream_.tellg();
    stream_.seekg(0, std::ios::beg);
  }

  arrow::Result<int64_t> Tell() const override {
    int64_t pos = stream_.tellg();
    if (!stream_.good()) {
      return arrow::Result<int64_t>(arrow::Status::IOError("IOError"));
    }
    return arrow::Result<int64_t>(pos);
  }

  bool closed() const override { return false; }

  arrow::Status Close() override { Fail("Close is not implemented for ParquetInputProxy"); }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    stream_.read(static_cast<char*>(out), nbytes);
    return arrow::Result(stream_.gcount());
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    arrow::BufferBuilder builder;
    RETURN_NOT_OK(builder.Reserve(nbytes));
    stream_.read(reinterpret_cast<char*>(builder.mutable_data()), nbytes);
    builder.UnsafeAdvance(stream_.gcount());
    return builder.Finish();
  }

  arrow::Status Seek(int64_t position) override {
    stream_.seekg(static_cast<std::streamoff>(position), std::ios::beg);
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> GetSize() override { return arrow::Result<int64_t>(object_size_); }

  mutable skyrise::ObjectReaderStream stream_;
  size_t object_size_;
};

}  // namespace

namespace skyrise {

ParquetFormatReader::ParquetFormatReader(std::unique_ptr<ObjectReader> source, Configuration configuration)
    : configuration_(std::move(configuration)) {
  auto input_stream = std::make_shared<ParquetInputProxy>(std::move(source));
  try {
    auto file_source = arrow::dataset::FileSource(input_stream);
    auto parquet_format = std::make_shared<arrow::dataset::ParquetFileFormat>();

    auto fragment = parquet_format->MakeFragment(file_source).ValueOrDie();

    auto scan_options = std::make_shared<arrow::dataset::ScanOptions>();
    scan_options->use_threads = false;
    scan_options->batch_size = kChunkDefaultSize;

    auto maybe_parquet_schema = parquet_format->Inspect(file_source);
    HANDLE_RESULT(maybe_parquet_schema, maybe_parquet_schema.status().ToString());
    auto parquet_schema = maybe_parquet_schema.ValueOrDie();

    auto scan_builder =
        std::make_shared<arrow::dataset::ScannerBuilder>(parquet_schema, std::move(fragment), scan_options);

    scanner_ = scan_builder->Finish().ValueOrDie();
    batch_iterator_ = scanner_->ScanBatches().ValueOrDie();

    ExtractSchema(parquet_schema);

    if (configuration_.expected_schema) {
      if (*schema_ != *configuration_.expected_schema) {
        SetError(StorageError(StorageErrorType::kInvalidArgument, "Unexpected schema found."));
      }
    }

  } catch (const std::logic_error& error) {
    SetError(StorageError(StorageErrorType::kIOError, error.what()));
  } catch (const parquet::ParquetInvalidOrCorruptedFileException& error) {
    SetError(StorageError(StorageErrorType::kIOError, error.what()));
  }
}

bool ParquetFormatReader::HasNext() {
  // We can only check whether the iterator has next batch if we call Next().
  // So, we have to store the result, and/or return true,
  // if HasNext() has been called already without the result being consumed.
  if (!iterator_has_next_) {
    return false;
  }
  if (!next_batch_) {
    auto iterator_result = batch_iterator_.Next();
    HANDLE_RESULT(iterator_result, "Failed to read from iterator");
    auto batch = std::move(iterator_result.ValueUnsafe().record_batch);
    if (!batch) {
      iterator_has_next_ = false;
      return false;
    }
    next_batch_ = std::move(batch);
  }
  return true;
}

std::unique_ptr<Chunk> ParquetFormatReader::Next() {
  if (!iterator_has_next_) {
    return nullptr;
  }

  Segments segments;
  std::shared_ptr<arrow::RecordBatch> batch;

  // Read next prepared batch from iterator.
  // If HasNext() has been called, we can get the batch from next_batch_.
  if (!next_batch_) {
    auto iterator_result = batch_iterator_.Next();
    batch = iterator_result.ValueUnsafe().record_batch;
  } else {
    batch = std::move(next_batch_);
  }

  // Convert each column of the arrow batch to a Skyrise segment
  // to build a skyrise chunk.
  for (int i = 0; i < batch->num_columns(); ++i) {
    auto column = batch->column(i);
    auto type_id = column->type_id();

    const auto typed_segment = ProcessArrowColumnToTypedSegment(column, type_id);
    segments.push_back(typed_segment);
  }
  return std::make_unique<Chunk>(std::move(segments));
}

std::shared_ptr<AbstractSegment> ParquetFormatReader::ProcessArrowColumnToTypedSegment(
    std::shared_ptr<arrow::Array>& column, arrow::Type::type& type_id) {
  switch (type_id) {
    case arrow::Type::FLOAT:
      return ArrowColumnToTypedSegment<float, arrow::FloatArray>(column);
    case arrow::Type::DOUBLE:
      return ArrowColumnToTypedSegment<double, arrow::DoubleArray>(column);
    case arrow::Type::BOOL:
      return ArrowColumnToTypedSegment<int32_t, arrow::Int8Array>(column);
    case arrow::Type::INT32:
      return ArrowColumnToTypedSegment<int32_t, arrow::Int32Array>(column);
    case arrow::Type::INT64:
    case arrow::Type::TIMESTAMP:
      return ArrowColumnToTypedSegment<int64_t, arrow::Int64Array>(column);
    case arrow::Type::BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
    case arrow::Type::STRING:
      return ArrowColumnToTypedSegment<std::string, arrow::StringArray>(column);
    case arrow::Type::LARGE_STRING:
      return ArrowColumnToTypedSegment<std::string, arrow::LargeStringArray>(column);
    case arrow::Type::DATE32:
      if (configuration_.parse_dates_as_string) {
        return ArrowDateColumnToStringSegment(column);
      }
      return ArrowColumnToTypedSegment<int64_t, arrow::Int32Array>(column);

    default:
      Fail("Encountered invalid type.");
  }
}

std::shared_ptr<AbstractSegment> ParquetFormatReader::ArrowDateColumnToStringSegment(
    std::shared_ptr<arrow::Array>& column) {
  auto arrow_array = std::static_pointer_cast<arrow::Int32Array>(column);
  std::vector<std::string> vector;
  auto length = column->length();
  vector.reserve(length);
  for (int i = 0; i < length; ++i) {
    // We can re-use the OrcTimestampToDateString logic here to convert from timestamp to readable date.
    vector.push_back(OrcFormatReader::OrcTimestampToDateString(arrow_array->Value(i)));
  }

  return std::make_shared<ValueSegment<std::string>>(std::move(vector));
}

template <typename BasicType, typename ArrowArrayType>
std::shared_ptr<AbstractSegment> ParquetFormatReader::ArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column) {
  auto arrow_array = std::static_pointer_cast<ArrowArrayType>(column);
  std::vector<BasicType> vector;
  auto length = column->length();
  vector.reserve(length);
  for (int i = 0; i < length; ++i) {
    vector.emplace_back(arrow_array->Value(i));
  }

  return std::make_shared<ValueSegment<BasicType>>(std::move(vector));
}

void ParquetFormatReader::ExtractSchema(const std::shared_ptr<arrow::Schema>& parquet_schema) {
  auto table_definitions = std::make_shared<TableColumnDefinitions>();

  for (int i = 0; i < parquet_schema->num_fields(); ++i) {
    const DataType type = ArrowTypeToSkyriseType(parquet_schema->field(i)->type()->id());
    const std::string name = parquet_schema->field(i)->name();
    const bool nullable = parquet_schema->field(i)->nullable();

    table_definitions->emplace_back(name, type, nullable);
  }
  schema_ = std::move(table_definitions);
}

DataType ParquetFormatReader::ArrowTypeToSkyriseType(const arrow::Type::type& type) {
  switch (type) {
    case arrow::Type::FLOAT:
      return DataType::kFloat;
    case arrow::Type::DOUBLE:
      return DataType::kDouble;
    case arrow::Type::INT64:
    case arrow::Type::TIMESTAMP:
      return DataType::kLong;
    case arrow::Type::INT32:
      return DataType::kInt;
    case arrow::Type::DATE32:
      if (configuration_.parse_dates_as_string) {
        return DataType::kString;
      }
      return DataType::kLong;
    case arrow::Type::BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
    case arrow::Type::LARGE_STRING:
    case arrow::Type::STRING:
      return DataType::kString;
    case arrow::Type::BOOL:
      return DataType::kInt;
    default:
      Fail("Encountered invalid type.");
  }
}

}  // namespace skyrise
