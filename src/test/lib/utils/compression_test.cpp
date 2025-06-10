#include "utils/compression.hpp"

#include <gtest/gtest.h>

namespace skyrise {

TEST(CompressionUtilsTest, CompressDecompressTest) {
  std::string compression_input =
      "ThisIsACompressionTestThisIsACompressionTestThisIsACompressionTestThisIsACompressionTest";
  std::string compression_output = Compress(compression_input);
  GTEST_ASSERT_LE(compression_output.size(), compression_input.size());
  GTEST_ASSERT_GE(compression_output.size(), 0);
  std::string decompression_output = Decompress(compression_output, compression_input.size());
  GTEST_ASSERT_EQ(compression_input, decompression_output);
}

}  // namespace skyrise
