#include "storage/formats/csv_writer.hpp"

#include <memory>
#include <sstream>
#include <string>

#include "format_test_base.hpp"

namespace skyrise {

class CsvFormatWriterTest : public FormatterTest {
 protected:
  void SetUp() override {
    FormatterTest::SetUp();
    options_.include_headers = true;
    options_.field_separator = ",";
    options_.record_separator = "\n";
  }

  CsvFormatWriterOptions options_;
};

TEST_F(CsvFormatWriterTest, FormatChunkAsCSV) {
  std::stringstream output;
  std::shared_ptr<std::stringstream> output_ptr(&output, [](auto /*unused*/) {});

  CsvFormatWriter formatter(options_);
  formatter.SetOutputHandler([&output_ptr](const char* data, size_t size) { output_ptr->write(data, size); });

  formatter.Initialize(schema_);
  formatter.ProcessChunk(chunk_);
  formatter.Finalize();

  ASSERT_EQ("id,text\n4,Hello\n6,world\n3,!\n", output.str());
}

TEST_F(CsvFormatWriterTest, FloatingPointPrecision) {
  std::stringstream output;
  std::shared_ptr<std::stringstream> output_ptr(&output, [](auto /*unused*/) {});

  TableColumnDefinitions schema_floating_points;
  schema_floating_points.emplace_back("test_float", DataType::kFloat, false);
  schema_floating_points.emplace_back("test_double", DataType::kDouble, false);

  auto value_segment_float = std::make_shared<ValueSegment<float>>();
  value_segment_float->Append(1.17549434f);

  auto value_segment_double = std::make_shared<ValueSegment<double>>();
  value_segment_double->Append(2.2250738585072014);

  CsvFormatWriter formatter(options_);
  formatter.SetOutputHandler([&output_ptr](const char* data, size_t size) { output_ptr->write(data, size); });

  formatter.Initialize(schema_floating_points);
  formatter.ProcessChunk(std::make_shared<Chunk>(Segments({value_segment_float, value_segment_double})));
  formatter.Finalize();

  ASSERT_EQ("test_float,test_double\n1.17549431,2.2250738585072014\n", output.str());
}

}  // namespace skyrise
