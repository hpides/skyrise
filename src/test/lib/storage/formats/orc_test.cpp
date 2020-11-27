
#include "storage/formats/orc.hpp"

#include <memory>
#include <sstream>
#include <string>

#include "format_test_base.hpp"

namespace skyrise {

class OrcFormatterTest : public FormatterTest {};

TEST_F(OrcFormatterTest, FormatChunkAsOrc) {
  constexpr auto kOrcMagic = "ORC";
  std::stringstream output;
  std::shared_ptr<std::stringstream> output_ptr(&output, [](auto /*unused*/) {});

  ORCFormatterOptions options;
  options.compression_kind = orc::CompressionKind_NONE;

  ORCFormatter formatter(options);
  formatter.SetOutput(output_ptr);

  formatter.Initialize(schema_);
  formatter.ProcessChunk(*chunk_);
  formatter.Finalize();

  // We are going to check for the magic string, and since we chose uncompressed data, the string examples must be
  // contained in the output stream.
  const std::string& output_str = output.str();
  ASSERT_GE(output_str.size(), 7);
  ASSERT_EQ(output_str.substr(0, 3), kOrcMagic);
  ASSERT_EQ(output_str.substr(output_str.size() - 4, 3), kOrcMagic);
  ASSERT_NE(output_str.find(kStringExample1), std::string::npos);
  ASSERT_NE(output_str.find(kStringExample2), std::string::npos);
  ASSERT_NE(output_str.find(kColumn1Name), std::string::npos);
  ASSERT_NE(output_str.find(kColumn2Name), std::string::npos);
}

}  // namespace skyrise
