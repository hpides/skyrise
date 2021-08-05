#pragma once

#include <limits>
#include <optional>
#include <vector>

#include <orc/OrcFile.hh>

#include "abstract_chunk_reader.hpp"
#include "storage/backend/abstract_storage.hpp"

namespace skyrise {

struct OrcFormatReaderOptions {
  bool parse_dates_as_string = false;
  std::shared_ptr<TableColumnDefinitions> expected_schema = nullptr;

  /**
   * You can either select certain rows or certain partitions by specifying an interval of indexes [lower; upper].
   * Partitions can only be selected if special meta data was added to the file. See OrcWriter for more details.
   */
  std::optional<std::pair<size_t, size_t>> select_row_range = std::nullopt;
  std::optional<std::pair<size_t, size_t>> select_partition_range = std::nullopt;
};

/**
 * OrcFormatReader reads chunks of data from a given ORC file.
 *
 * Data read from ORC files will be converted to an appropriate DataType. This class does not support files with complex
 * types such as arrays and will fail parsing those files. This class also does not support null-values. If null-values
 * are present, default values (0 or empty string) will be returned instead. This class is not thread-safe.
 */
class OrcFormatReader : public AbstractChunkReader {
 public:
  using Configuration = OrcFormatReaderOptions;
  explicit OrcFormatReader(std::unique_ptr<ObjectReader> source, Configuration configuration = Configuration());

  static std::string OrcTimestampToDateString(int32_t num_days_since_1970);
  static DataType OrcTypeKindToDataType(orc::TypeKind type, bool date_as_string = false);

  bool HasNext() override;
  std::unique_ptr<Chunk> Next() override;

 protected:
  void ExtractSchema();
  std::vector<size_t> ExtractPartitionInformation();
  void SeekToSelectedRows();
  Configuration configuration_;

  std::unique_ptr<orc::Reader> reader_;
  std::unique_ptr<orc::RowReader> row_reader_;
  std::unique_ptr<orc::ColumnVectorBatch> column_vector_batch_;

 private:
  size_t num_rows_read_ = 0;
  size_t read_at_most_num_rows_ = std::numeric_limits<size_t>::max();
};

}  // namespace skyrise
