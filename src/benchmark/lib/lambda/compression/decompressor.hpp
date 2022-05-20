#pragma once

#include "compressor.hpp"

namespace skyrise {

// orc::SeekableInputStream
class OrcSeekableInputStreamFacade {
 public:
  virtual ~OrcSeekableInputStreamFacade() = default;
  virtual bool Next(const void** data, int* size) = 0;
};

class Decompressor : public DataPullProcessor {
 public:
  static constexpr size_t kCompressionBlockSize = 64 * 1024;
  static constexpr size_t kBufferCapacity = 1024 * 1024;

  virtual bool Process();

 protected:
  std::unique_ptr<OrcSeekableInputStreamFacade> uncompressed_stream_ = nullptr;
};

class NoneDecompressor : public Decompressor {
 public:
  bool Process();
};

class ZlibDecompressor : public Decompressor {
 public:
  bool Process();
};

class ZstdDecompressor : public Decompressor {
 public:
  bool Process();
};

class Lz4Decompressor : public Decompressor {
 public:
  bool Process();
};

}  // namespace skyrise
