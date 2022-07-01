#include <gtest/gtest.h>

#include "lambda/compression/compressor.hpp"
#include "lambda/compression/decompressor.hpp"
#include "utils/literal.hpp"
#include "utils/string.hpp"

namespace skyrise {

template <typename CompressorClass, typename DecompressorClass>
struct CompressionTypeDefinitions {  // NOLINT(altera-struct-pack-align)
  using Compressor = CompressorClass;
  using Decompressor = DecompressorClass;
};

template <typename T>
class CompressionTest : public ::testing::Test {};

template <typename T>
class CompressionTradeoffTest : public ::testing::Test {};

using CompressionTypes = ::testing::Types<CompressionTypeDefinitions<NoneCompressor, NoneDecompressor>,
                                          CompressionTypeDefinitions<ZlibCompressor, ZlibDecompressor>,
                                          CompressionTypeDefinitions<ZstdCompressor, ZstdDecompressor>,
                                          CompressionTypeDefinitions<Lz4Compressor, Lz4Decompressor>>;

using CompressionTradeoffTypes = ::testing::Types<CompressionTypeDefinitions<ZlibCompressor, ZlibDecompressor>,
                                                  CompressionTypeDefinitions<ZstdCompressor, ZstdDecompressor>,
                                                  CompressionTypeDefinitions<Lz4Compressor, Lz4Decompressor>>;

TYPED_TEST_SUITE(CompressionTest, CompressionTypes, );
TYPED_TEST_SUITE(CompressionTradeoffTest, CompressionTradeoffTypes, );

TYPED_TEST(CompressionTest, CompressDecompress) {
  // Compress
  typename TypeParam::Compressor compressor;
  constexpr size_t kDataLength = 5_MB + 7;  // Something that is not dividable by block size.
  std::string input_data = RandomString(kDataLength);
  std::stringstream compressed_data;

  compressor.SetOutput([&compressed_data](const char* data, size_t length) { compressed_data.write(data, length); });

  for (size_t i = 0; i < kDataLength; i += 16_KB) {
    compressor.Process(&input_data.c_str()[i], std::min(static_cast<size_t>(16_KB), kDataLength - i));
  }

  compressor.Finish();

  // Decompress
  compressed_data.seekg(0);
  std::vector<char> buffer;
  buffer.reserve(16_KB);

  typename TypeParam::Decompressor decompressor;
  std::stringstream decompressed_data;

  decompressor.SetOutput(
      [&decompressed_data](const char* data, size_t length) { decompressed_data.write(data, length); });
  decompressor.SetInput([&compressed_data, &buffer](const char** data, size_t* length) {
    compressed_data.read(buffer.data(), buffer.capacity());
    *data = buffer.data();
    *length = compressed_data.gcount();
  });
  decompressor.Process();

  EXPECT_EQ(input_data, decompressed_data.str());
}

TYPED_TEST(CompressionTradeoffTest, CompressDecompress) {
  typename TypeParam::Compressor compressor_favor_compression(false);
  typename TypeParam::Compressor compressor_favor_speed(true);

  // Something that is not dividable by block size.
  // Generate a pattern that can be compressed easily.
  std::vector<char> input_data(5_MB + 7, 'a');

  std::stringstream compressed_data_favor_compression;
  std::stringstream compressed_data_favor_speed;

  compressor_favor_compression.SetOutput([&compressed_data_favor_compression](const char* data, size_t length) {
    compressed_data_favor_compression.write(data, length);
  });
  compressor_favor_speed.SetOutput([&compressed_data_favor_speed](const char* data, size_t length) {
    compressed_data_favor_speed.write(data, length);
  });

  compressor_favor_compression.Process(input_data.data(), input_data.size());
  compressor_favor_speed.Process(input_data.data(), input_data.size());

  compressor_favor_compression.Finish();
  compressor_favor_speed.Finish();

  // There is no guarantee that the respective compression configuration actually yields a smaller size or higher speed.
  EXPECT_LE(compressed_data_favor_compression.str().size(), input_data.size());
  EXPECT_LE(compressed_data_favor_speed.str().size(), input_data.size());
}

}  // namespace skyrise
