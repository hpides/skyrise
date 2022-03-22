#pragma once

#include <orc/OrcFile.hh>

#include "processor.hpp"
#include "utils/literal.hpp"

namespace skyrise {

// OrcBufferedOutputStreamFacade wraps orc::BufferedOutputStream to hide the dependency to protobuf.
class OrcBufferedOutputStreamFacade {
 public:
  virtual ~OrcBufferedOutputStreamFacade() = default;
  virtual bool Next(void** data, int* size) = 0;
  virtual void BackUp(int count) = 0;
  virtual uint64_t flush() = 0;
};

class ProxyOutputStream : public orc::OutputStream {
 public:
  uint64_t getLength() const;
  uint64_t getNaturalWriteSize() const;
  void write(const void* buf, size_t length);
  const std::string& getName() const;
  void close();

  void setCallback(std::function<void(const char* data, size_t length)> callback);

 private:
  static constexpr size_t kNaturalWriteSize = 20_MB;
  inline static const std::string kName = "ProxyOutputStream";
  std::function<void(const char* data, size_t length)> callback_;
  size_t num_bytes_written_ = 0;
};

class Compressor : public DataPushProcessor {
 public:
  static constexpr size_t kCompressionBlockSize = 64_KB;
  static constexpr size_t kBufferCapacity = 1_MB;

  bool Process(const char* data, size_t length);
  bool Finish();
  void SetOutput(std::function<void(const char* data, size_t length)> callback);

 protected:
  ProxyOutputStream output_;
  std::unique_ptr<OrcBufferedOutputStreamFacade> compression_stream_ = nullptr;
};

class NoneCompressor : public Compressor {
 public:
  NoneCompressor();
};

class ZlibCompressor : public Compressor {
 public:
  ZlibCompressor(bool favor_speed_over_compression = false);
};

class ZstdCompressor : public Compressor {
 public:
  ZstdCompressor(bool favor_speed_over_compression = false);
};

class Lz4Compressor : public Compressor {
 public:
  Lz4Compressor(bool favor_speed_over_compression = false);
};

}  // namespace skyrise
