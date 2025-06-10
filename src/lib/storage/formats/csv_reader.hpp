#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "abstract_chunk_reader.hpp"
#include "configuration.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/table/table_column_definition.hpp"
#include "utils/literal.hpp"

namespace skyrise {

struct CsvFormatReaderOptions {
  // The read buffer determines the granularity in which in-memory resident data is consumed. It is preferable to align
  // it with the request buffer size. In case the sizes differ, more copying is involved.
  size_t read_buffer_size = kS3ReadRequestSizeBytes;
  char delimiter = ',';
  bool guess_delimiter = true;
  bool guess_has_header = true;
  bool guess_has_types = true;
  bool has_header = false;
  bool has_types = false;
  std::shared_ptr<TableColumnDefinitions> expected_schema = nullptr;

  bool operator==(const CsvFormatReaderOptions& rhs) const {
    const bool same_schema = [&]() {
      if (expected_schema && rhs.expected_schema) {
        return *expected_schema == *rhs.expected_schema;
      } else {
        return !expected_schema && !rhs.expected_schema;
      }
    }();
    return read_buffer_size == rhs.read_buffer_size && delimiter == rhs.delimiter &&
           guess_delimiter == rhs.guess_delimiter && guess_has_header == rhs.guess_has_header &&
           has_header == rhs.has_header && has_types == rhs.has_types && same_schema;
  }
};

/*
 * CsvFormatReader reads formatted data from text files.
 */
class CsvFormatReader : public AbstractChunkReader {
 public:
  using Configuration = CsvFormatReaderOptions;
  using Lines = std::vector<std::string_view>;
  using Columns = std::vector<std::vector<std::string_view>>;

  explicit CsvFormatReader(std::shared_ptr<ObjectBuffer> source, size_t object_size,
                           Configuration configuration = Configuration());

  bool HasNext() override;
  std::unique_ptr<Chunk> Next() override;

  static char GuessDelimiter(const Lines& lines);
  static bool GuessHasHeader(const Columns& columns);
  static bool GuessHasTypeInformation(const Columns& columns);

 protected:
  void ExtractColumns();
  void InitialSetup();
  void BuildSchema();
  void BuildColumnNames(TableColumnDefinitions* schema);
  void BuildColumnTypes(TableColumnDefinitions* schema);
  StorageError FillBuffer();

 private:
  Columns columns_;
  Configuration configuration_;
  std::shared_ptr<ObjectBuffer> source_;
  std::vector<char> buffer_;
  size_t chunk_offset_ = 0;
  bool read_full_file_ = false;
  size_t num_ignore_lines_in_next_chunk_ = 0;
};

}  // namespace skyrise
