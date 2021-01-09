#pragma once

#include <sstream>
#include <string>

#include "abstract_formatter.hpp"
#include "storage/types/chunk.hpp"
#include "storage/types/table_column_definition.hpp"

namespace skyrise {

// There is no support for escaping yet. This means that `field_separator` and `record_separator` may
// not be included in the data itself. Keep in mind that TPC-H data can contain "," which is why we chose
// ";" to be the default field separator.

struct CsvFormatterOptions {
  std::string field_separator = ";";
  std::string record_separator = "\n";
  bool include_headers = true;
};

class CsvFormatter : public AbstractFormatter {
 public:
  using Configuration = CsvFormatterOptions;

  CsvFormatter(CsvFormatterOptions options = CsvFormatterOptions());

  void Initialize(const TableColumnDefinitions& schema) override;
  void ProcessChunk(const Chunk& chunk) override;
  void Finalize() override;

 private:
  CsvFormatterOptions options_;
  size_t num_fields_ = 0;
};
}  // namespace skyrise
