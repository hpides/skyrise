#include "compressor.hpp"

// We use non-public headers to be as close as possible to the liborc implementation.
#include <Compression.hh>

namespace skyrise {

class OrcBufferedOutputStreamFacadeImplementation : public OrcBufferedOutputStreamFacade {
 public:
  explicit OrcBufferedOutputStreamFacadeImplementation(std::unique_ptr<orc::BufferedOutputStream> stream)
      : stream_(std::move(stream)) {}
  bool Next(void** data, int* size) override { return stream_->Next(data, size); }
  void BackUp(int count) override { return stream_->BackUp(count); }
  uint64_t flush() override { return stream_->flush(); }

 private:
  std::unique_ptr<orc::BufferedOutputStream> stream_;
};

uint64_t ProxyOutputStream::getLength() const { return num_bytes_written_; }

uint64_t ProxyOutputStream::getNaturalWriteSize() const { return kNaturalWriteSize; }

void ProxyOutputStream::write(const void* buf, size_t length) {
  num_bytes_written_ += length;
  callback_(static_cast<const char*>(buf), length);
}

const std::string& ProxyOutputStream::getName() const { return kName; }

void ProxyOutputStream::close() {}

void ProxyOutputStream::setCallback(std::function<void(const char* data, size_t length)> callback) {
  callback_ = std::move(callback);
}

void Compressor::SetOutput(std::function<void(const char* data, size_t length)> callback) {
  DataPushProcessor::SetOutput(callback);
  output_.setCallback(callback);
}

bool Compressor::Process(const char* data, size_t length) {
  void* buffer = nullptr;
  int buffer_size = 0;
  size_t written = 0;
  int write_next = 0;
  while (written < length) {
    // We receive a pointer to a buffer that we can write our data to.
    if (!compression_stream_->Next(&buffer, &buffer_size)) {
      return false;
    }

    // We cannot write more bytes that the given buffer can hold.
    write_next = std::min(static_cast<size_t>(buffer_size), length - written);
    std::memcpy(buffer, &data[written], write_next);
    written += write_next;
  }
  // If we do not fill the last buffer completely, we need to back up accordingly.
  if (write_next != buffer_size) {
    compression_stream_->BackUp(buffer_size - write_next);
  }

  return true;
}

bool Compressor::Finish() {
  compression_stream_->flush();
  return true;
}

NoneCompressor::NoneCompressor() {
  compression_stream_ = std::make_unique<OrcBufferedOutputStreamFacadeImplementation>(
      orc::createCompressor(orc::CompressionKind_NONE, &output_, orc::CompressionStrategy_SPEED, kBufferCapacity,
                            kCompressionBlockSize, *orc::getDefaultPool()));
}

ZlibCompressor::ZlibCompressor(bool favor_speed_over_compression) {
  compression_stream_ = std::make_unique<OrcBufferedOutputStreamFacadeImplementation>(orc::createCompressor(
      orc::CompressionKind_ZLIB, &output_,
      favor_speed_over_compression ? orc::CompressionStrategy_SPEED : orc::CompressionStrategy_COMPRESSION,
      kBufferCapacity, kCompressionBlockSize, *orc::getDefaultPool()));
}

ZstdCompressor::ZstdCompressor(bool favor_speed_over_compression) {
  compression_stream_ = std::make_unique<OrcBufferedOutputStreamFacadeImplementation>(orc::createCompressor(
      orc::CompressionKind_ZSTD, &output_,
      favor_speed_over_compression ? orc::CompressionStrategy_SPEED : orc::CompressionStrategy_COMPRESSION,
      kBufferCapacity, kCompressionBlockSize, *orc::getDefaultPool()));
}

Lz4Compressor::Lz4Compressor(bool favor_speed_over_compression) {
  compression_stream_ = std::make_unique<OrcBufferedOutputStreamFacadeImplementation>(orc::createCompressor(
      orc::CompressionKind_LZ4, &output_,
      favor_speed_over_compression ? orc::CompressionStrategy_SPEED : orc::CompressionStrategy_COMPRESSION,
      kBufferCapacity, kCompressionBlockSize, *orc::getDefaultPool()));
}

}  // namespace skyrise
