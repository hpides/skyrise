#include "orc.hpp"

namespace skyrise {

ORCFormatter::ORCFormatter(ORCFormatterOptions config)
    : output_proxy_([this](const char* data, size_t length) { WriteToOutput(data, length); }),
      config_(std::move(config)) {}

std::unique_ptr<orc::Type> ORCFormatter::SkyriseTypeToOrcType(DataType type) {
  switch (type) {
    case DataType::kFloat:
      return orc::createPrimitiveType(orc::FLOAT);
    case DataType::kDouble:
      return orc::createPrimitiveType(orc::DOUBLE);
    case DataType::kInt:
      return orc::createPrimitiveType(orc::INT);
    case DataType::kLong:
      return orc::createPrimitiveType(orc::LONG);
    case DataType::kString:
      return orc::createPrimitiveType(orc::STRING);
    case DataType::kNull:
      Fail("NULL is not a supported column type.");
    default:
      Fail("Unknown column type encountered.");
  }
}

void ORCFormatter::Initialize(const TableColumnDefinitions& schema) {
  type_ = orc::createStructType();
  for (const TableColumnDefinition& column : schema) {
    type_->addStructField(column.name, SkyriseTypeToOrcType(column.data_type));
  }

  orc::WriterOptions options;
  options.setCompression(config_.compression_kind);
  options.setCompressionStrategy(config_.compression_strategy);
  options.setStripeSize(config_.stripe_size);

  writer_ = orc::createWriter(*type_, &output_proxy_, options);
}

void ORCFormatter::ProcessChunk(const Chunk& chunk) {
  if (!batch_ || batch_->capacity < chunk.Size()) {
    batch_ = writer_->createRowBatch(chunk.Size());
  }

  auto* struct_vector = dynamic_cast<orc::StructVectorBatch*>(batch_.get());
  struct_vector->numElements = chunk.Size();
  for (size_t i = 0; i < chunk.GetColumnCount(); i++) {
    CopySegmentToOrcColumn(chunk.GetSegment(i), struct_vector->fields[i]);
  }

  writer_->add(*batch_);
}

void ORCFormatter::Finalize() {
  writer_->close();
  writer_.reset();
}

void ORCFormatter::CopySegmentToOrcColumn(const std::shared_ptr<AbstractSegment>& segment,
                                          orc::ColumnVectorBatch* orc_column) {
  orc_column->numElements = segment->Size();
  switch (segment->GetDataType()) {
    case DataType::kLong:
      GenericCopySegmentToOrcColumn(dynamic_cast<ValueSegment<int64_t>*>(segment.get()),
                                    dynamic_cast<orc::LongVectorBatch*>(orc_column));
      return;
    case DataType::kInt:
      GenericCopySegmentToOrcColumn(dynamic_cast<ValueSegment<int32_t>*>(segment.get()),
                                    dynamic_cast<orc::LongVectorBatch*>(orc_column));
      return;
    case DataType::kFloat:
      GenericCopySegmentToOrcColumn(dynamic_cast<ValueSegment<float>*>(segment.get()),
                                    dynamic_cast<orc::DoubleVectorBatch*>(orc_column));
      return;
    case DataType::kDouble:
      GenericCopySegmentToOrcColumn(dynamic_cast<ValueSegment<double>*>(segment.get()),
                                    dynamic_cast<orc::DoubleVectorBatch*>(orc_column));
      return;
    case DataType::kString:
      GenericCopySegmentToOrcColumn(dynamic_cast<ValueSegment<std::string>*>(segment.get()),
                                    dynamic_cast<orc::StringVectorBatch*>(orc_column));
      return;
    default:
      Fail("Invalid type found.");
  }
}

template <typename SegmentType, typename VectorBatchType>
void ORCFormatter::GenericCopySegmentToOrcColumn(SegmentType* segment, VectorBatchType* batch) {
  auto& segment_values = segment->Values();

  batch->hasNulls = segment->IsNullable();
  for (size_t i = 0; i < segment->Size(); i++) {
    batch->data[i] = segment_values[i];
    if (segment->IsNullable()) {
      batch->notNull[i] = segment->NullValues()[i] ? 0 : 1;
    }
  }
}

template <>
void ORCFormatter::GenericCopySegmentToOrcColumn(ValueSegment<std::string>* segment, orc::StringVectorBatch* batch) {
  // String is special, because we need to store all strings concatenated inside `batch->blob`.
  // In `batch->data` we store pointers to the first character of the string.
  // Finally `batch->length` holds the number of bytes for every string.

  auto& segment_values = segment->Values();

  size_t total_string_bytes = 0;
  for (const auto& value : segment_values) {
    total_string_bytes += value.size();
  }

  batch->blob.resize(total_string_bytes);

  size_t bytes_copied = 0;
  batch->hasNulls = segment->IsNullable();
  for (size_t i = 0; i < segment->Size(); i++) {
    std::memcpy(&batch->blob.data()[bytes_copied], segment_values[i].c_str(), segment_values[i].size());
    batch->data[i] = &batch->blob.data()[bytes_copied];
    batch->length[i] = segment_values[i].size();
    bytes_copied += segment_values[i].size();
    if (segment->IsNullable()) {
      batch->notNull[i] = segment->NullValues()[i] ? 0 : 1;
    }
  }
}

}  // namespace skyrise
