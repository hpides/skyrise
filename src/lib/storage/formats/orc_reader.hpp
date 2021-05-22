#pragma once

#include <orc/OrcFile.hh>

#include "abstract_format_reader.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/backend/stream.hpp"

namespace skyrise {

struct OrcFormatReaderOptions {
  bool parse_dates_as_string = false;
  std::shared_ptr<TableColumnDefinitions> expected_schema = nullptr;
};

/**
 * OrcFormatReader reads chunks of data from a given ORC file.
 *
 * Data read from ORC files will be converted to an appropriate DataType. This class does not support files with complex
 * types such as arrays and will fail parsing those files. This class also does not support null-values. If null-values
 * are present, default values (0 or empty string) will be returned instead. This class is not thread-safe.
 */
class OrcFormatReader : public AbstractFormatReader {
 public:
  using Configuration = OrcFormatReaderOptions;
  explicit OrcFormatReader(std::unique_ptr<ObjectReader> source, Configuration configuration = Configuration());

  static std::string OrcTimestampToDateString(int32_t num_days_since_1970);
  static DataType OrcTypeKindToDataType(orc::TypeKind type, bool date_as_string = false);

  bool HasNext() override;
  std::unique_ptr<Chunk> Next() override;

 protected:
  void ExtractSchema();
  Configuration configuration_;

  std::unique_ptr<orc::Reader> reader_;
  std::unique_ptr<orc::RowReader> row_reader_;
  std::unique_ptr<orc::ColumnVectorBatch> column_vector_batch_;

 private:
  size_t num_rows_read_ = 0;
};

}  // namespace skyrise
