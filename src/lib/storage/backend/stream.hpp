#pragma once

#include <cstring>
#include <iostream>
#include <optional>
#include <streambuf>
#include <vector>

#include "abstract_storage.hpp"
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
  explicit ObjectReaderStream(std::unique_ptr<ObjectReader> reader);

 private:
  ObjectReaderStreamBuffer stream_buffer_;
};

}  // namespace skyrise
