#pragma once

#include <limits>
#include <optional>
#include <vector>

#include <arrow/dataset/scanner.h>
#include <parquet/arrow/reader.h>

#include "abstract_chunk_reader.hpp"
#include "boost/container/vector.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/io_handle/object_buffer.hpp"

namespace skyrise {

class ParquetInputProxy : public arrow::io::RandomAccessFile {
 public:
  explicit ParquetInputProxy(std::shared_ptr<skyrise::ObjectBuffer> source, size_t object_size);

  arrow::Result<int64_t> Tell() const override;

  bool closed() const override;

  arrow::Status Close() override;

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

  arrow::Status Seek(int64_t position) override;

  arrow::Result<int64_t> GetSize() override;

 private:
  std::shared_ptr<skyrise::ObjectBuffer> source_;
  std::shared_ptr<skyrise::ByteBuffer> active_buffer_;
  size_t offset_ = 0;
  size_t object_size_;
};

struct ParquetFormatReaderOptions {
  bool parse_dates_as_string = false;
  std::shared_ptr<TableColumnDefinitions> expected_schema = nullptr;
  std::optional<std::vector<ColumnId>> include_columns = std::nullopt;
  std::optional<std::vector<int32_t>> row_group_ids = std::nullopt;
  std::optional<arrow::compute::Expression> arrow_expression = std::nullopt;
  std::optional<std::shared_ptr<AbstractPredicateExpression>> skyrise_expression = std::nullopt;
};

class ParquetFormatReader : public AbstractChunkReader {
 public:
  using Configuration = ParquetFormatReaderOptions;
  explicit ParquetFormatReader(std::shared_ptr<ObjectBuffer> source, size_t object_size,
                               Configuration configuration = Configuration());

  bool HasNext() override;
  std::unique_ptr<Chunk> Next() override;

 protected:
  void ExtractSchema(const std::shared_ptr<arrow::Schema>& arrow_schema);

  std::shared_ptr<AbstractSegment> ProcessArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column,
                                                                    arrow::Type::type& type_id);

  template <typename BasicType, typename ArrowArrayType>
  std::shared_ptr<AbstractSegment> ArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column);
  static std::shared_ptr<AbstractSegment> ArrowDateColumnToStringSegment(std::shared_ptr<arrow::Array>& column);
  static std::shared_ptr<AbstractSegment> ArrowFixedSizeBinaryColumnToStringSegment(
      std::shared_ptr<arrow::Array>& column);

  static std::string DaysSince1970ToDateString(int32_t num_days_since_1970);

  DataType ArrowTypeToSkyriseType(const arrow::Type::type& type) const;

  Configuration configuration_;

 private:
  std::shared_ptr<arrow::dataset::Scanner> scanner_;
  arrow::dataset::TaggedRecordBatchIterator batch_iterator_;

  // Initialized to true as we first have to call Next() on the batch_iterator to know that there is something to read.
  bool iterator_has_next_ = true;
  std::shared_ptr<arrow::RecordBatch> next_batch_;
};

}  // namespace skyrise
