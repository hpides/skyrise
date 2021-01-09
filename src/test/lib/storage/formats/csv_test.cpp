#include "storage/formats/csv.hpp"

#include <memory>
#include <sstream>
#include <string>

#include "format_test_base.hpp"

namespace skyrise {

class CsvFormatterTest : public FormatterTest {};

TEST_F(CsvFormatterTest, FormatChunkAsCSV) {
  std::stringstream output;
  std::shared_ptr<std::stringstream> output_ptr(&output, [](auto /*unused*/) {});

  CsvFormatterOptions options;
  options.include_headers = true;
  options.field_separator = ",";
  options.record_separator = "\n";

  CsvFormatter formatter(options);
  formatter.SetOutput(output_ptr);

  formatter.Initialize(schema_);
  formatter.ProcessChunk(*chunk_);
  formatter.Finalize();

  ASSERT_EQ("id,text\n4,Hello\n6,world\n3,!\n", output.str());
}

}  // namespace skyrise
