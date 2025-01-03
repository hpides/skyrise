#include "orc_reader.hpp"

#include <arrow/array.h>
#include <arrow/dataset/file_orc.h>

namespace skyrise {

OrcFormatReader::OrcFormatReader(std::shared_ptr<ObjectBuffer> source, size_t object_size,
                                 const Configuration& configuration)
    : configuration_(configuration) {
  auto proxy = std::make_shared<OrcInputProxy>(std::move(source), object_size);
  try {
    auto file_source = arrow::dataset::FileSource(proxy);
    auto orc_format = std::make_shared<arrow::dataset::OrcFileFormat>();
    auto maybe_fragment = orc_format->MakeFragment(file_source);
    Assert(maybe_fragment.ok(), maybe_fragment.status().message());
    std::shared_ptr<arrow::dataset::Fragment> fragment = maybe_fragment.ValueUnsafe();

    auto scan_options = std::make_shared<arrow::dataset::ScanOptions>();
    scan_options->use_threads = false;
    scan_options->batch_size = kChunkDefaultSize;

    auto maybe_orc_schema = orc_format->Inspect(file_source);
    Assert(maybe_orc_schema.ok(), maybe_orc_schema.status().message());
    auto orc_schema = maybe_orc_schema.ValueOrDie();
    auto scan_builder = std::make_shared<arrow::dataset::ScannerBuilder>(orc_schema, std::move(fragment), scan_options);

    // Push down any available projections.
    if (configuration_.include_columns.has_value()) {
      const auto field_names = orc_schema->field_names();
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

    ExtractSchema(orc_schema);

    if (configuration_.expected_schema) {
      if (*schema_ != *configuration_.expected_schema) {
        SetError(StorageError(StorageErrorType::kInvalidArgument, "Unexpected schema found."));
      }
    }

  } catch (const std::logic_error& error) {
    SetError(StorageError(StorageErrorType::kIOError, error.what()));
  }
}

bool OrcFormatReader::HasNext() {
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

std::unique_ptr<Chunk> OrcFormatReader::Next() {
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

std::shared_ptr<AbstractSegment> OrcFormatReader::ProcessArrowColumnToTypedSegment(
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

template <typename BasicType, typename ArrowArrayType>
std::shared_ptr<AbstractSegment> OrcFormatReader::ArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column) {
  auto arrow_array = std::static_pointer_cast<ArrowArrayType>(column);
  std::vector<BasicType> values;

  const int64_t length = column->length();
  values.reserve(length);
  for (int i = 0; i < length; ++i) {
    values.emplace_back(arrow_array->Value(i));
  }

  return std::make_shared<ValueSegment<BasicType>>(std::move(values));
}

std::shared_ptr<AbstractSegment> OrcFormatReader::ArrowDateColumnToStringSegment(
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

std::shared_ptr<AbstractSegment> OrcFormatReader::ArrowFixedSizeBinaryColumnToStringSegment(
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

std::string OrcFormatReader::DaysSince1970ToDateString(int32_t num_days_since_1970) {
  const time_t seconds_since_1970 = static_cast<time_t>(num_days_since_1970) * static_cast<time_t>(60 * 60 * 24);
  tm calendar_date{};
  std::array<char, 11> buffer = {0};  // YYYY-mm-dd + '\0'
  gmtime_r(&seconds_since_1970, &calendar_date);
  strftime(buffer.data(), 11, "%Y-%m-%d", &calendar_date);

  return {buffer.data()};
}

void OrcFormatReader::ExtractSchema(const std::shared_ptr<arrow::Schema>& arrow_schema) {
  auto table_definitions = std::make_shared<TableColumnDefinitions>();

  for (int i = 0; i < arrow_schema->num_fields(); ++i) {
    const DataType type = ArrowTypeToSkyriseType(arrow_schema->field(i)->type()->id());
    const std::string name = arrow_schema->field(i)->name();
    const bool nullable = arrow_schema->field(i)->nullable();

    table_definitions->emplace_back(name, type, nullable);
  }
  schema_ = std::move(table_definitions);
}

DataType OrcFormatReader::ArrowTypeToSkyriseType(const arrow::Type::type& type) const {
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
