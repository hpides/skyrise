#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "abstract_chunk_reader.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/table/table_column_definition.hpp"
#include "utils/literal.hpp"

namespace skyrise {

struct CsvFormatReaderOptions {
  size_t read_buffer_size = 20_MB;
  char delimiter = ',';
  bool guess_delimiter = true;
  bool guess_has_header = true;
  bool guess_has_types = true;
  bool has_header = false;
  bool has_types = false;
  std::shared_ptr<TableColumnDefinitions> expected_schema = nullptr;
};

/*
 * CsvFormatReader reads formatted data from text files.
 */
class CsvFormatReader : public AbstractChunkReader {
 public:
  using Configuration = CsvFormatReaderOptions;
  using Lines = std::vector<std::string_view>;
  using Columns = std::vector<std::vector<std::string_view>>;

  explicit CsvFormatReader(std::unique_ptr<ObjectReader> source, Configuration configuration = Configuration());

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
  std::unique_ptr<ObjectReader> source_;
  std::vector<char> buffer_;
  size_t chunk_offset_ = 0;
  bool read_full_file_ = false;
  size_t num_ignore_lines_in_next_chunk_ = 0;
};

}  // namespace skyrise
