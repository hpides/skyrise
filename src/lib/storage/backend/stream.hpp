#pragma once

#include <cstring>
#include <iostream>
#include <optional>
#include <streambuf>
#include <vector>

#include "abstract_storage.hpp"
#include "storage/io_handle/byte_buffer.hpp"
#include "utils/literal.hpp"

namespace skyrise {

/**
 * ObjectReaderStreamBuffer provides a stream buffer that can be used to construct an std::i(o)stream. This class owns
 * an internal buffer and is concerned with refilling it from the given ObjectReader. The object size is needed to
 * provide seeking capabilities.
 */
class ObjectReaderStreamBuffer : public std::streambuf {
 public:
  explicit ObjectReaderStreamBuffer(std::unique_ptr<ObjectReader> reader);

  /**
   * Fills the internal buffer with data from the end of the object. This function is used to optimize requests for file
   * formats that start by reading data from the end of an object.
   */
  void FillBufferWithTail();

 protected:
  /**
   * Refill the internal buffer.
   */
  int underflow() override;

  /**
   * Adjust data pointers given an absolute stream position.
   */
  pos_type seekpos(pos_type pos, std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override;

  /**
   * Adjust data pointers given a relative stream position. This function translates down to seekpos.
   */
  pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                   std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override;

 private:
  size_t GetObjectSize();

  static constexpr size_t kBufferSize = 20_MB;
  static constexpr auto kStreamLoggingTag = "ObjectReaderStreamBuffer";

  std::unique_ptr<ObjectReader> reader_;
  std::optional<size_t> object_size_;  // This value is obtained lazily.
  std::vector<char> buffer_;
  size_t current_offset_ = 0;
};

/**
 * ObjectReaderStream offers stream functionality on top of any ObjectReader. Besides compatibility with C++ streams, it
 * also provides buffering, which is particularly valuable for objects read from remote storage (e.g., Amazon S3).
 * Multiple read operations on data located close to each other will likely translate into one single request on the
 * ObjectReader. While this class only implements read operations, it inherits from std::iostream to serve as body in
 * Aws::Http::HttpRequest.
 */
class ObjectReaderStream : public std::iostream {
 public:
  explicit ObjectReaderStream(std::unique_ptr<ObjectReader> reader, bool initial_fill_buffer_with_tail = false);

 private:
  ObjectReaderStreamBuffer stream_buffer_;
};

/**
 * A DelegateStreamBuffer reads and writes data from and to an externally owned std::vector. It will resize the vector
 * accordingly.
 */
class DelegateStreamBuffer : public std::streambuf {
 public:
  DelegateStreamBuffer() = default;

  explicit DelegateStreamBuffer(ByteBuffer* buffer);
  void Reset(ByteBuffer* buffer);

 protected:
  std::streamsize xsputn(const char* s, std::streamsize n) override;
  pos_type seekpos(pos_type pos, std::ios::openmode which = std::ios::in | std::ios::out) override;
  pos_type seekoff(off_type off, std::ios::seekdir dir,
                   std::ios::openmode which = std::ios::in | std::ios::out) override;

  int overflow(int ch = traits_type::eof()) override;

 private:
  ByteBuffer* buffer_ = nullptr;
};

}  // namespace skyrise
