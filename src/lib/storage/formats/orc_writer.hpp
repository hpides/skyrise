#pragma once

#include <arrow/adapters/orc/adapter.h>
#include <arrow/adapters/orc/options.h>

#include "abstract_chunk_writer.hpp"

namespace skyrise {

class OrcOutputProxy : public arrow::io::OutputStream {
  using WriteCallback = std::function<void(const char* data, size_t length)>;

 public:
  explicit OrcOutputProxy(WriteCallback callback) : callback_(std::move(callback)) {}
  arrow::Status Close() override {
    is_closed_ = true;
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> Tell() const override { return {position_}; }
  bool closed() const override { return is_closed_; }
  arrow::Status Write(const void* data, int64_t nbytes) override {
    position_ += nbytes;
    callback_(static_cast<const char*>(data), nbytes);
    return arrow::Status::OK();
  }

 private:
  int64_t position_ = 0;
  bool is_closed_ = false;
  WriteCallback callback_;
};

struct OrcFormatWriterOptions {
  arrow::Compression::type compression = arrow::Compression::ZSTD;
  arrow::adapters::orc::CompressionStrategy compression_strategy = arrow::adapters::orc::CompressionStrategy::kSpeed;
  int64_t stripe_size = static_cast<int64_t>(64 * 1024 * 1024);  // ORC default.
};

class OrcFormatWriter : public AbstractFormatWriter {
 public:
  explicit OrcFormatWriter(OrcFormatWriterOptions options);
  void Initialize(const TableColumnDefinitions& schema) override;
  void ProcessChunk(std::shared_ptr<const Chunk> chunk) override;
  void Finalize() override;

 private:
  static std::shared_ptr<arrow::Schema> BuildSchema(const TableColumnDefinitions& schema);
  static std::shared_ptr<arrow::DataType> SkyriseTypeToOrcType(DataType type);

  template <class ArrowType, typename ValueSegmentType>
  arrow::Result<std::shared_ptr<arrow::Array>> ValueSegmentToArray(ValueSegmentType* segment);
  arrow::Result<std::shared_ptr<arrow::Array>> CopySegmentToArrowArray(const std::shared_ptr<AbstractSegment>& segment);

  OrcFormatWriterOptions options_;
  OrcOutputProxy output_proxy_;
  std::unique_ptr<arrow::adapters::orc::ORCFileWriter> writer_;
  std::shared_ptr<arrow::Schema> schema_;
};

}  // namespace skyrise
