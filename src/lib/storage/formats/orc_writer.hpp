#pragma once

#include <orc/OrcFile.hh>

#include "abstract_format_writer.hpp"
#include "storage/table/value_segment.hpp"
#include "utils/literal.hpp"

namespace skyrise {

namespace detail {

class OrcOutputProxy : public orc::OutputStream {
 public:
  using WriteCallback = std::function<void(const char* data, size_t length)>;

  OrcOutputProxy(WriteCallback callback) : callback_(callback) {}

  uint64_t getLength() const override { return bytes_written_; }
  uint64_t getNaturalWriteSize() const override { return kWriteChunkSize; }
  void write(const void* buf, size_t length) override {
    bytes_written_ += length;
    callback_(static_cast<const char*>(buf), length);
  }
  void close() override {}
  const std::string& getName() const override { return kName; }

 private:
  inline static const std::string kName{"OrcOutputProxy"};
  static constexpr uint64_t kWriteChunkSize = 16_MB;
  uint64_t bytes_written_ = 0;
  WriteCallback callback_;
};

}  // namespace detail

struct OrcFormatWriterOptions {
  orc::CompressionKind compression_kind = orc::CompressionKind_NONE;
  orc::CompressionStrategy compression_strategy = orc::CompressionStrategy_SPEED;
  size_t stripe_size = 64_MB;  // orc default
};

class OrcFormatWriter : public AbstractFormatWriter {
 public:
  using Configuration = OrcFormatWriterOptions;
  explicit OrcFormatWriter(OrcFormatWriterOptions config);

  void Initialize(const TableColumnDefinitions& schema) override;
  void ProcessChunk(std::shared_ptr<Chunk> chunk) override;
  void Finalize() override;

  void AddMetadata(std::string key, std::string value);

 private:
  static std::unique_ptr<orc::Type> SkyriseTypeToOrcType(DataType type);
  static void CopySegmentToOrcColumn(const std::shared_ptr<AbstractSegment>& segment,
                                     orc::ColumnVectorBatch* orc_column);
  template <typename SegmentT, typename VectorBatchT>
  static void GenericCopySegmentToOrcColumn(SegmentT* segment, VectorBatchT* batch);

  detail::OrcOutputProxy output_proxy_;
  OrcFormatWriterOptions config_;
  std::unique_ptr<orc::Type> type_;
  std::unique_ptr<orc::Writer> writer_;
  std::unique_ptr<orc::ColumnVectorBatch> batch_;
};

// This specialization needs to be in the same scope as OrcFormatter
template <>
void OrcFormatWriter::GenericCopySegmentToOrcColumn(ValueSegment<std::string>* segment, orc::StringVectorBatch* batch);

}  // namespace skyrise
