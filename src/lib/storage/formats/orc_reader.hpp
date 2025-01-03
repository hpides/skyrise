#pragma once

#include <arrow/dataset/scanner.h>

#include "abstract_chunk_reader.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class OrcInputProxy : public arrow::io::RandomAccessFile {
 public:
  explicit OrcInputProxy(std::shared_ptr<ObjectBuffer> source, size_t object_size)
      : source_(std::move(source)), object_size_(object_size) {}

  arrow::Status Close() override {
    is_closed_ = true;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override { return {position_}; }

  bool closed() const override { return is_closed_; }

  arrow::Status Seek(int64_t position) override {
    position_ = position;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    auto buffer_view = source_->Read(position_, nbytes);
    std::memcpy(out, buffer_view->Data(), nbytes);

    if (static_cast<int64_t>(buffer_view->Size()) != nbytes) {
      return {arrow::Status::IOError("Error while reading from ORC file.")};
    }
    return {buffer_view->Size()};
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    auto buffer_view = source_->Read(position_, nbytes);

    if (static_cast<int64_t>(buffer_view->Size()) != nbytes) {
      return {arrow::Status::IOError("Error while reading from ORC file.")};
    }
    return arrow::MutableBuffer::Wrap(buffer_view->Data(), nbytes);
  }

  arrow::Result<int64_t> GetSize() override { return {object_size_}; }

 private:
  std::shared_ptr<ObjectBuffer> source_;
  size_t object_size_;
  int64_t position_ = 0;
  bool is_closed_ = false;
};

struct OrcFormatReaderOptions {
  bool parse_dates_as_string = false;
  std::optional<std::vector<ColumnId>> include_columns = std::nullopt;
  std::shared_ptr<TableColumnDefinitions> expected_schema = nullptr;
  bool operator==(const OrcFormatReaderOptions& rhs) const {
    const bool same_schema = [this, &rhs]() {
      if (expected_schema && rhs.expected_schema) {
        return *expected_schema == *rhs.expected_schema;
      } else {
        return !expected_schema && !rhs.expected_schema;
      }
    }();
    return parse_dates_as_string == rhs.parse_dates_as_string && same_schema;
  }
};

class OrcFormatReader : public AbstractChunkReader {
 public:
  using Configuration = OrcFormatReaderOptions;

  OrcFormatReader(std::shared_ptr<ObjectBuffer> source, size_t object_size,
                  const Configuration& configuration = Configuration());
  bool HasNext() override;
  std::unique_ptr<Chunk> Next() override;
  void ExtractSchema(const std::shared_ptr<arrow::Schema>& arrow_schema);
  DataType ArrowTypeToSkyriseType(const arrow::Type::type& type) const;

  std::shared_ptr<AbstractSegment> ProcessArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column,
                                                                    arrow::Type::type& type_id);
  static std::string DaysSince1970ToDateString(int32_t num_days_since_1970);

  template <typename BasicType, typename ArrowArrayType>
  std::shared_ptr<AbstractSegment> ArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column);
  static std::shared_ptr<AbstractSegment> ArrowDateColumnToStringSegment(std::shared_ptr<arrow::Array>& column);
  static std::shared_ptr<AbstractSegment> ArrowFixedSizeBinaryColumnToStringSegment(
      std::shared_ptr<arrow::Array>& column);

 private:
  Configuration configuration_;
  std::shared_ptr<arrow::dataset::Scanner> scanner_;
  arrow::dataset::TaggedRecordBatchIterator batch_iterator_;
  bool iterator_has_next_ = true;
  std::shared_ptr<arrow::RecordBatch> next_batch_;
};

}  // namespace skyrise
