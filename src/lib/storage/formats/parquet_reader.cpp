#include "parquet_reader.hpp"

#include <numeric>
#include <string>

#include <arrow/array.h>
#include <arrow/array/array_binary.h>
#include <arrow/buffer_builder.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <magic_enum/magic_enum.hpp>
#include <parquet/properties.h>
#include <parquet/stream_reader.h>

#include "expression/value_expression.hpp"
#include "storage/backend/stream.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

ParquetInputProxy::ParquetInputProxy(std::shared_ptr<skyrise::ObjectBuffer> source, size_t object_size)
    : source_(std::move(source)), object_size_(object_size) {}

arrow::Result<int64_t> ParquetInputProxy::Tell() const { return {offset_}; }

bool ParquetInputProxy::closed() const { return false; }

arrow::Status ParquetInputProxy::Close() { Fail("Close is not implemented for ParquetInputProxy"); }

arrow::Result<int64_t> ParquetInputProxy::Read(int64_t nbytes, void* out) {
  auto buffer_view = source_->Read(offset_, nbytes);
  std::memcpy(out, buffer_view->Data(), nbytes);

  if (std::cmp_not_equal(buffer_view->Size(), nbytes)) {
    return {arrow::Status::IOError("IOError read bytes mismatch expected=" + std::to_string(nbytes) +
                                   " read=" + std::to_string(buffer_view->Size()))};
  }
  return arrow::Result(buffer_view->Size());
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ParquetInputProxy::Read(int64_t nbytes) {
  auto buffer_view = source_->Read(offset_, nbytes);

  if (std::cmp_not_equal(buffer_view->Size(), nbytes)) {
    return {arrow::Status::IOError("IOError read bytes mismatch expected=" + std::to_string(nbytes) +
                                   " read=" + std::to_string(buffer_view->Size()))};
  }
  return {std::make_shared<arrow::Buffer>(buffer_view->Data(), nbytes)};
}

arrow::Status ParquetInputProxy::Seek(int64_t position) {
  offset_ = position;
  return arrow::Status::OK();
}

arrow::Result<int64_t> ParquetInputProxy::GetSize() { return {object_size_}; }

ParquetFormatReader::ParquetFormatReader(std::shared_ptr<ObjectBuffer> source, size_t object_size,
                                         Configuration configuration)
    : configuration_(std::move(configuration)) {
  auto input_stream = std::make_shared<ParquetInputProxy>(std::move(source), object_size);
  try {
    auto file_source = arrow::dataset::FileSource(input_stream);
    auto parquet_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    std::shared_ptr<arrow::dataset::ParquetFileFragment> parquet_fragment =
        std::static_pointer_cast<arrow::dataset::ParquetFileFragment>(
            parquet_format->MakeFragment(file_source).ValueOrDie());

    // Read only specific partitions if row group ids are provided.
    if (configuration_.row_group_ids.has_value()) {
      auto row_group_subset = parquet_fragment->Subset(configuration_.row_group_ids.value());
      Assert(row_group_subset.ok(), row_group_subset.status().ToString());
      parquet_fragment = std::static_pointer_cast<arrow::dataset::ParquetFileFragment>(row_group_subset.ValueOrDie());
    }

    auto scan_options = std::make_shared<arrow::dataset::ScanOptions>();
    scan_options->use_threads = false;
    scan_options->batch_size = kChunkDefaultSize;

    auto maybe_parquet_schema = parquet_format->Inspect(file_source);
    Assert(maybe_parquet_schema.ok(), maybe_parquet_schema.status().ToString());
    auto parquet_schema = maybe_parquet_schema.ValueOrDie();

    auto scan_builder =
        std::make_shared<arrow::dataset::ScannerBuilder>(parquet_schema, std::move(parquet_fragment), scan_options);

    // Push down any available predicates.
    if (configuration_.arrow_expression.has_value()) {
      Assert(scan_builder->Filter(*configuration_.arrow_expression).ok(), "Failed to evaluate predicate");
    }

    // Push down any available projections.
    if (configuration_.include_columns.has_value()) {
      const auto field_names = parquet_schema->field_names();
      const auto columns = *configuration_.include_columns;
      std::vector<std::string> include_columns;
      include_columns.reserve(columns.size());

      for (const auto column_id : columns) {
        include_columns.emplace_back(field_names[column_id]);
      }

      Assert(scan_builder->Project(include_columns).ok(), "Failed to project columns");
    }

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
    Assert(iterator_result.ok(), iterator_result.status().message());
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
    Assert(iterator_result.ok(), iterator_result.status().message());
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
      return ArrowColumnToTypedSegment<int32_t, arrow::BooleanArray>(column);
    case arrow::Type::INT32:
      return ArrowColumnToTypedSegment<int32_t, arrow::Int32Array>(column);
    case arrow::Type::INT64:
    case arrow::Type::TIMESTAMP:
      return ArrowColumnToTypedSegment<int64_t, arrow::Int64Array>(column);
    case arrow::Type::BINARY:
    case arrow::Type::STRING:
      return ArrowColumnToTypedSegment<std::string, arrow::StringArray>(column);
    case arrow::Type::LARGE_STRING:
      return ArrowColumnToTypedSegment<std::string, arrow::LargeStringArray>(column);
    case arrow::Type::FIXED_SIZE_BINARY:
      return ArrowFixedSizeBinaryColumnToStringSegment(column);
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
  const auto arrow_array = std::static_pointer_cast<arrow::Int32Array>(column);
  std::vector<std::string> string_values;

  const int64_t length = column->length();
  string_values.reserve(length);
  for (int i = 0; i < length; ++i) {
    // We can re-use the OrcTimestampToDateString logic here to convert from timestamp to readable date.
    string_values.push_back(DaysSince1970ToDateString(arrow_array->Value(i)));
  }

  return std::make_shared<ValueSegment<std::string>>(std::move(string_values));
}

std::shared_ptr<AbstractSegment> ParquetFormatReader::ArrowFixedSizeBinaryColumnToStringSegment(
    std::shared_ptr<arrow::Array>& column) {
  const auto binary_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
  std::vector<std::string> string_values;

  const int64_t length = column->length();
  string_values.reserve(length);
  for (int i = 0; i < length; ++i) {
    string_values.emplace_back(binary_array->GetString(i));
  }

  return std::make_shared<ValueSegment<std::string>>(std::move(string_values));
}

template <typename BasicType, typename ArrowArrayType>
std::shared_ptr<AbstractSegment> ParquetFormatReader::ArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column) {
  auto arrow_array = std::static_pointer_cast<ArrowArrayType>(column);
  std::vector<BasicType> values;

  const int64_t length = column->length();
  values.reserve(length);
  for (int i = 0; i < length; ++i) {
    values.emplace_back(arrow_array->Value(i));
  }

  return std::make_shared<ValueSegment<BasicType>>(std::move(values));
}

void ParquetFormatReader::ExtractSchema(const std::shared_ptr<arrow::Schema>& arrow_schema) {
  auto table_definitions = std::make_shared<TableColumnDefinitions>();

  for (int i = 0; i < arrow_schema->num_fields(); ++i) {
    const DataType type = ArrowTypeToSkyriseType(arrow_schema->field(i)->type()->id());
    const std::string name = arrow_schema->field(i)->name();
    const bool nullable = arrow_schema->field(i)->nullable();

    table_definitions->emplace_back(name, type, nullable);
  }
  schema_ = std::move(table_definitions);
}

DataType ParquetFormatReader::ArrowTypeToSkyriseType(const arrow::Type::type& type) const {
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

std::string ParquetFormatReader::DaysSince1970ToDateString(int32_t num_days_since_1970) {
  const time_t seconds_since_1970 = static_cast<time_t>(num_days_since_1970) * static_cast<time_t>(60 * 60 * 24);
  tm calendar_date{};
  std::array<char, 11> buffer = {0};  // YYYY-mm-dd + '\0'
  gmtime_r(&seconds_since_1970, &calendar_date);
  strftime(buffer.data(), 11, "%Y-%m-%d", &calendar_date);

  return {buffer.data()};
}

}  // namespace skyrise
