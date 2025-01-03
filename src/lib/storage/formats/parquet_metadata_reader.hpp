#pragma once

#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/io/file.h>

#include "constants.hpp"
#include "parquet_reader.hpp"

namespace skyrise {

class ParquetFormatMetadataReader : public ParquetFormatReader {
 public:
  explicit ParquetFormatMetadataReader(const std::shared_ptr<ObjectBuffer>& source, const size_t object_size,
                                       const Configuration& configuration = Configuration(),
                                       const size_t footer_length = kParquetFooterSizeBytes);

  static std::vector<std::pair<size_t, size_t>> CalculatePageOffsets(
      const std::shared_ptr<ObjectReader>& object_reader, const size_t object_size,
      const Configuration& configuration = Configuration(), const size_t footer_length = kParquetFooterSizeBytes);

 private:
  std::vector<std::pair<size_t, size_t>> CalculatePageOffsetsForColumnIds();

  static void ComputeByteRange(parquet::FileMetaData* file_metadata, int64_t source_size, int row_group_index,
                               int column_index, std::vector<std::pair<size_t, size_t>>& ranges);

  std::shared_ptr<arrow::dataset::ParquetFileFragment> parquet_fragment_;
  size_t object_size_;
  static constexpr int64_t kMaxDictionaryHeaderSize = 100;
};

}  //  namespace skyrise
