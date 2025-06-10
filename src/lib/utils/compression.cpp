#include "compression.hpp"

#include <memory>

#include <arrow/util/compression.h>

#include "assert.hpp"
#include "string.hpp"

constexpr arrow::Compression::type kCompressionType = arrow::Compression::type::GZIP;

namespace skyrise {

std::string Compress(const std::string& to_compress) {
  // Using one-shot compression, which is available with snappy.
  auto codec_result = arrow::util::Codec::Create(kCompressionType);
  Assert(codec_result.ok(), "Could not create codec");
  std::unique_ptr<arrow::util::Codec> codec = std::move(*codec_result);

  // Prepare input and output
  const auto* const to_compress_data = reinterpret_cast<const uint8_t*>(to_compress.data());
  const size_t max_compression_size = codec->MaxCompressedLen(to_compress.size(), to_compress_data);
  std::string output;
  output.resize(max_compression_size);
  auto* output_data = reinterpret_cast<uint8_t*>(output.data());

  // Compress
  auto compression_result = codec->Compress(to_compress.size(), to_compress_data, max_compression_size, output_data);
  Assert(compression_result.ok(), "Could not compress data");
  size_t compression_size = *compression_result;

  output.resize(compression_size);

  return output;
}

std::string Decompress(const std::string& to_decompress, size_t decompressed_size) {
  // The Arrow decompression tools are somewhat flawed:
  // A lot of the compression algorithms that naturally would support streaming AND one-shot decompression do only allow
  // one mode in the toolset. For one-shot decompression, we require the decompressed size of the input. While a lot of
  // compression algorithms encode this, Arrow does not offer an interface for this (which is not good). This makes
  // allowing all algorithms of the toolset really cumbersome. We thus only support two algorithms: Snappy and GZIP.
  // Snappy, because it is really fast and gzip, because it offers stronger compression and is widely used.
  // We use one-shot decompression for both of them, since the arrow snappy interface does not allow streaming
  // decompression.
  Assert(kCompressionType == arrow::Compression::type::SNAPPY || kCompressionType == arrow::Compression::type::GZIP,
         "Currently, only Snappy and Gzip are supported for decompression");

  Assert(decompressed_size != 0 || kCompressionType == arrow::Compression::type::SNAPPY,
         "Currently, only Snappy does support decompression without given decompressed size");

  auto codec_result = arrow::util::Codec::Create(kCompressionType);
  Assert(codec_result.ok(), "Could not create codec");

  std::unique_ptr<arrow::util::Codec> codec = std::move(*codec_result);
  const auto* const to_decompress_data = reinterpret_cast<const uint8_t*>(to_decompress.data());

  arrow::Result<int64_t> decompress_result;
  if (kCompressionType == arrow::Compression::SNAPPY) {
    // Snappy does store the decompression size, but arrow does not provided this functionality.
    // We can still work around this by using the status of an initial compression, where the size is encoded.
    decompress_result = codec->Decompress(to_decompress.size(), to_decompress_data, 0, nullptr);
    std::string status = decompress_result.status().message();

    // Form of status: "Output buffer size (", output_buffer_len, ") must be ",decompressed_size, " or larger.");
    decompressed_size = std::strtol(SplitStringByDelimiter(status, ' ')[6].c_str(), nullptr, 10);
  }

  // prepare output
  std::string output;
  output.resize(decompressed_size);
  auto* output_data = reinterpret_cast<uint8_t*>(output.data());
  decompress_result = codec->Decompress(to_decompress.size(), to_decompress_data, decompressed_size, output_data);
  Assert(decompress_result.ok(), "Decompression failed.");
  return output;
}

}  // namespace skyrise
