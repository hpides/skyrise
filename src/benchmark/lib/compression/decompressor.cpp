#include "decompressor.hpp"

// We use non-public headers to be as close as possible to the liborc implementation.
#include <Compression.hh>

namespace skyrise {

// This class only implements the bare minimum to work for compression.
class PullingInputStream : public orc::SeekableInputStream {
 public:
  explicit PullingInputStream(std::function<void(const char** data, size_t* length)> callback)
      : callback_(std::move(callback)) {}
  void seek(orc::PositionProvider& /*position*/) override{/* We do not support seeking here */};
  [[nodiscard]] std::string getName() const override { return kName_; };
  void BackUp(int /*count*/) override {}
  bool Skip(int /*count*/) override { return false; }
  [[nodiscard]] int64_t ByteCount() const override { return current_position_; }
  bool Next(const void** data, int* size) override {
    size_t wide_size = 0;
    callback_(reinterpret_cast<const char**>(data), &wide_size);
    *size = static_cast<int>(wide_size);
    return wide_size != 0;
  }

 private:
  const std::string kName_ = "PullingInputStream";
  std::function<void(const char** data, size_t* length)> callback_;
  size_t current_position_ = 0;
};

class OrcSeekableInputStreamImplementation : public OrcSeekableInputStreamFacade {
 public:
  explicit OrcSeekableInputStreamImplementation(std::unique_ptr<orc::SeekableInputStream> stream)
      : stream_(std::move(stream)) {}
  bool Next(const void** data, int* size) override { return stream_->Next(data, size); }

 private:
  std::unique_ptr<orc::SeekableInputStream> stream_;
};

bool Decompressor::Process() {
  const char* buffer = nullptr;
  int buffer_size = 0;
  while (uncompressed_stream_->Next(reinterpret_cast<const void**>(&buffer), &buffer_size)) {
    if (buffer_size > 0) {
      output_(buffer, buffer_size);
    }
  }
  return true;
}

bool NoneDecompressor::Process() {
  uncompressed_stream_ = std::make_unique<OrcSeekableInputStreamImplementation>(
      orc::createDecompressor(orc::CompressionKind_NONE, std::make_unique<PullingInputStream>(input_), kBufferCapacity,
                              *orc::getDefaultPool()));
  return Decompressor::Process();
}

bool ZlibDecompressor::Process() {
  uncompressed_stream_ = std::make_unique<OrcSeekableInputStreamImplementation>(
      orc::createDecompressor(orc::CompressionKind_ZLIB, std::make_unique<PullingInputStream>(input_), kBufferCapacity,
                              *orc::getDefaultPool()));
  return Decompressor::Process();
}

bool ZstdDecompressor::Process() {
  uncompressed_stream_ = std::make_unique<OrcSeekableInputStreamImplementation>(
      orc::createDecompressor(orc::CompressionKind_ZSTD, std::make_unique<PullingInputStream>(input_), kBufferCapacity,
                              *orc::getDefaultPool()));
  return Decompressor::Process();
}

bool Lz4Decompressor::Process() {
  uncompressed_stream_ = std::make_unique<OrcSeekableInputStreamImplementation>(orc::createDecompressor(
      orc::CompressionKind_LZ4, std::make_unique<PullingInputStream>(input_), kBufferCapacity, *orc::getDefaultPool()));
  return Decompressor::Process();
}

}  // namespace skyrise
