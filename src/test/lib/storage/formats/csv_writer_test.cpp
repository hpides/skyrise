#include "storage/formats/csv_writer.hpp"

#include <memory>
#include <sstream>
#include <string>

#include "format_test_base.hpp"

namespace skyrise {

class CsvFormatWriterTest : public FormatterTest {};

TEST_F(CsvFormatWriterTest, FormatChunkAsCSV) {
  std::stringstream output;
  std::shared_ptr<std::stringstream> output_ptr(&output, [](auto /*unused*/) {});

  CsvFormatWriterOptions options;
  options.include_headers = true;
  options.field_separator = ",";
  options.record_separator = "\n";

  CsvFormatWriter formatter(options);
  formatter.SetOutputHandler([&output_ptr](const char* data, size_t size) { output_ptr->write(data, size); });

  formatter.Initialize(schema_);
  formatter.ProcessChunk(chunk_);
  formatter.Finalize();

  ASSERT_EQ("id,text\n4,Hello\n6,world\n3,!\n", output.str());
}

}  // namespace skyrise
