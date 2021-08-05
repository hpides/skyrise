#pragma once

#include <sstream>
#include <string>

#include "abstract_chunk_writer.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table_column_definition.hpp"

namespace skyrise {

// There is no support for escaping yet. This means that `field_separator` and `record_separator` may
// not be included in the data itself. Keep in mind that TPC-H data can contain "," which is why we chose
// ";" to be the default field separator.

struct CsvFormatWriterOptions {
  std::string field_separator = ";";
  std::string record_separator = "\n";
  bool include_headers = true;
};

class CsvFormatWriter : public AbstractFormatWriter {
 public:
  using Configuration = CsvFormatWriterOptions;

  explicit CsvFormatWriter(CsvFormatWriterOptions options = CsvFormatWriterOptions());

  void Initialize(const TableColumnDefinitions& schema) override;
  void ProcessChunk(std::shared_ptr<const Chunk> chunk) override;
  void Finalize() override;

 private:
  CsvFormatWriterOptions options_;
  size_t num_fields_ = 0;
};
}  // namespace skyrise
