#pragma once

#include <limits>
#include <optional>
#include <vector>

#include <arrow/compute/exec/options.h>
#include <arrow/dataset/scanner.h>
#include <parquet/arrow/reader.h>

#include "abstract_chunk_reader.hpp"
#include "boost/container/vector.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "storage/backend/abstract_storage.hpp"

namespace skyrise {

struct ParquetFormatReaderOptions {
  bool parse_dates_as_string = false;
  std::shared_ptr<TableColumnDefinitions> expected_schema = nullptr;
  std::optional<std::vector<ColumnId>> include_columns = std::nullopt;
  std::optional<arrow::compute::Expression> arrow_expression = std::nullopt;
};

class ParquetFormatReader : public AbstractChunkReader {
 public:
  using Configuration = ParquetFormatReaderOptions;
  explicit ParquetFormatReader(std::unique_ptr<ObjectReader> source, Configuration configuration = Configuration());

  bool HasNext() override;
  std::unique_ptr<Chunk> Next() override;

 protected:
  void ExtractSchema(const std::shared_ptr<arrow::Schema>& arrow_schema);

  std::shared_ptr<AbstractSegment> ProcessArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column,
                                                                    arrow::Type::type& type_id);

  template <typename BasicType, typename ArrowArrayType>
  std::shared_ptr<AbstractSegment> ArrowColumnToTypedSegment(std::shared_ptr<arrow::Array>& column);
  std::shared_ptr<AbstractSegment> ArrowDateColumnToStringSegment(std::shared_ptr<arrow::Array>& column);

  DataType ArrowTypeToSkyriseType(const arrow::Type::type& type);

  Configuration configuration_;

 private:
  std::unique_ptr<parquet::ParquetFileReader> file_reader_;
  std::unique_ptr<parquet::arrow::FileReader> arrow_file_reader_;
  std::shared_ptr<::arrow::RecordBatchReader> record_batch_reader_;
  std::shared_ptr<arrow::dataset::Scanner> scanner_;
  arrow::dataset::TaggedRecordBatchIterator batch_iterator_;

  // Initialized to true as we first have to call Next() on the batch_iterator to know that there is something to read.
  bool iterator_has_next_ = true;
  std::shared_ptr<arrow::RecordBatch> next_batch_;
};

}  // namespace skyrise
