#include "orc_writer.hpp"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/table.h"
#include "arrow/util/key_value_metadata.h"
#include "serialization/binary_serialization_stream.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

OrcFormatWriter::OrcFormatWriter(OrcFormatWriterOptions options)
    : options_(options), output_proxy_([this](const char* data, size_t length) { WriteToOutput(data, length); }) {}

void skyrise::OrcFormatWriter::Initialize(const TableColumnDefinitions& schema) {
  schema_ = BuildSchema(schema);

  auto write_options = arrow::adapters::orc::WriteOptions();
  write_options.compression = options_.compression;
  write_options.compression_strategy = options_.compression_strategy;
  write_options.stripe_size = options_.stripe_size;
  writer_ = arrow::adapters::orc::ORCFileWriter::Open(&output_proxy_, write_options).ValueOrDie();
}

void skyrise::OrcFormatWriter::ProcessChunk(std::shared_ptr<const Chunk> chunk) {
  std::vector<std::shared_ptr<arrow::Array>> columns;
  for (size_t i = 0; i < chunk->GetColumnCount(); ++i) {
    auto segment = chunk->GetSegment(i);
    columns.emplace_back(CopySegmentToArrowArray(segment).ValueOrDie());
  }

  auto record_batch = arrow::RecordBatch::Make(schema_, chunk->Size(), columns);
  arrow::Status status = writer_->Write(*record_batch);
}

template <class BuilderType, typename ValueSegmentType>
arrow::Result<std::shared_ptr<arrow::Array>> OrcFormatWriter::ValueSegmentToArray(ValueSegmentType* segment) {
  BuilderType builder;
  auto& segment_values = segment->Values();
  auto& null_values = segment->NullValues();

  if (segment->IsNullable()) {
    for (size_t i = 0; i < segment->Size(); ++i) {
      ARROW_RETURN_NOT_OK(null_values[i] ? builder.AppendNull() : builder.Append(segment_values[i]));
    }
  } else {
    for (size_t i = 0; i < segment->Size(); ++i) {
      ARROW_RETURN_NOT_OK(builder.Append(segment_values[i]));
    }
  }
  return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>> OrcFormatWriter::CopySegmentToArrowArray(
    const std::shared_ptr<AbstractSegment>& segment) {
  switch (segment->GetDataType()) {
    case DataType::kLong:
      return ValueSegmentToArray<arrow::Int64Builder>(dynamic_cast<ValueSegment<int64_t>*>(segment.get()));
    case DataType::kInt:
      return ValueSegmentToArray<arrow::Int32Builder>(dynamic_cast<ValueSegment<int32_t>*>(segment.get()));
    case DataType::kFloat:
      return ValueSegmentToArray<arrow::FloatBuilder>(dynamic_cast<ValueSegment<float>*>(segment.get()));
    case DataType::kDouble:
      return ValueSegmentToArray<arrow::DoubleBuilder>(dynamic_cast<ValueSegment<double>*>(segment.get()));
    case DataType::kString:
      return ValueSegmentToArray<arrow::StringBuilder>(dynamic_cast<ValueSegment<std::string>*>(segment.get()));
    default:
      Fail("Invalid type found.");
  }
}

void skyrise::OrcFormatWriter::Finalize() { arrow::Status status = writer_->Close(); }

std::shared_ptr<arrow::DataType> OrcFormatWriter::SkyriseTypeToOrcType(DataType type) {
  switch (type) {
    case skyrise::DataType::kFloat:
      return arrow::float32();
    case skyrise::DataType::kDouble:
      return arrow::float64();
    case skyrise::DataType::kInt:
      return arrow::int32();
    case skyrise::DataType::kLong:
      return arrow::int64();
    case skyrise::DataType::kString:
      return arrow::utf8();
    case skyrise::DataType::kNull:
      Fail("NULL is not a supported column type.");
    default:
      Fail("Unknown column type encountered.");
  }
}

std::shared_ptr<arrow::Schema> OrcFormatWriter::BuildSchema(const TableColumnDefinitions& schema) {
  arrow::FieldVector fields;
  for (const TableColumnDefinition& column : schema) {
    auto field = std::make_shared<arrow::Field>(column.name, SkyriseTypeToOrcType(column.data_type), column.nullable);
    fields.emplace_back(std::move(field));
  }
  return std::make_shared<arrow::Schema>(std::move(fields));
}

}  // namespace skyrise
