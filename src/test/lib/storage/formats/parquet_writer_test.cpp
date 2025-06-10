#include "storage/formats/parquet_writer.hpp"

#include <memory>
#include <string>

#include "format_test_base.hpp"

namespace skyrise {

class ParquetFormatWriterTest : public FormatterTest {};

TEST_F(ParquetFormatWriterTest, FormatChunkAsParquet) {
  static const std::string kParquetMagicNumber = "PAR1";

  std::stringstream output;
  std::shared_ptr<std::stringstream> output_ptr(&output, [](auto /*unused*/) {});

  const ParquetFormatWriter::Configuration formatter_config{parquet::Compression::UNCOMPRESSED};
  ParquetFormatWriter formatter{formatter_config};

  formatter.SetOutputHandler([&output_ptr](const char* data, size_t size) { output_ptr->write(data, size); });

  formatter.Initialize(schema_);
  formatter.ProcessChunk(chunk_);
  formatter.Finalize();

  const std::string& output_str = output.str();
  ASSERT_GE(output_str.size(), 7);
  EXPECT_EQ(output_str.substr(0, 4), kParquetMagicNumber);
  EXPECT_EQ(output_str.substr(output_str.size() - 4, 4), kParquetMagicNumber);
  EXPECT_NE(output_str.find(kStringExample1), std::string::npos);
  EXPECT_NE(output_str.find(kStringExample2), std::string::npos);
  EXPECT_NE(output_str.find(kStringExample3), std::string::npos);
  EXPECT_NE(output_str.find(kColumn1Name), std::string::npos);
  EXPECT_NE(output_str.find(kColumn2Name), std::string::npos);
};

}  // namespace skyrise
