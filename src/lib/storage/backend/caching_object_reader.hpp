#pragma once

#include <functional>
#include <optional>
#include <set>
#include <vector>

#include "abstract_storage.hpp"
#include "utils/literal.hpp"

namespace skyrise {

/**
 * CacheableLocation is a byte range in an object representing a semantic unit of reading.
 */
class CacheableLocation {
 public:
  static CacheableLocation WithFirstLastByteInclusive(int64_t first_byte, int64_t last_byte);
  static CacheableLocation WithOffsetSize(int64_t offset, int64_t size);

  int64_t FirstByte() const { return offset_; }
  int64_t LastByteExclusive() const { return last_byte_ + 1; }
  int64_t LastByteInclusive() const { return last_byte_; }
  int64_t Offset() const { return offset_; }
  int64_t Size() const { return size_; }

  /**
   * We use CacheableLocation in a std::map/std::set and sort by offset.
   */
  bool operator<(const CacheableLocation& other) const;
  bool Includes(const CacheableLocation& other) const;

 private:
  /**
   * Use static functions above to construct a CacheableLocation.
   */
  CacheableLocation() = default;

  int64_t last_byte_ = 0;
  int64_t offset_ = 0;
  int64_t size_ = 0;
};

/**
 * CacheAccessPattern describes the intended access pattern. Given this information the cache can be filled
 * accordingly.
 */
enum class CacheAccessPattern {
  kRandom,
  kSequential,
};

/**
 * The CacheManager stores a set of CacheableLocations that serve as cacheable building blocks. Semantically those
 * building blocks describe areas that are meaningful to cache.
 */
class CacheManager {
 public:
  CacheManager& AddTail(int64_t min_bytes);
  CacheManager& SetAccessPattern(CacheAccessPattern access_pattern);
  int64_t Tail() { return tail_cache_; };
  CacheManager& SetSourceSize(int64_t size);

  CacheManager& AddLocation(const CacheableLocation& location);
  std::optional<CacheableLocation> CacheableLocationIncluding(const CacheableLocation& location,
                                                              int64_t desired_size) const;

 private:
  CacheableLocation AdjustedLocation(const CacheableLocation& requested_location,
                                     const CacheableLocation& cached_location, int64_t desired_size) const;
  static CacheableLocation ExtendedLocation(const CacheableLocation& requested_location,
                                            const CacheableLocation& cached_location, int64_t desired_size);
  static CacheableLocation MidpointLocation(const CacheableLocation& requested_location,
                                            const CacheableLocation& cached_location, int64_t desired_size);

  void AddTailIfPossible();

  int64_t tail_cache_ = 0;
  int64_t source_size_ = 0;

  /**
   * We assume random access per default.
   */
  CacheAccessPattern access_pattern_ = CacheAccessPattern::kRandom;

  /**
   * The range for the tail cache can only be set after the size of the source object is known. This flag stores,
   * whether is has been set already.
   */
  bool has_tail_ = false;
  std::multiset<CacheableLocation> locations_;
};

/**
 * A CachingObjectReader wraps an ObjectReader and maintains a cache based on the cacheable locations defined.
 */
class CachingObjectReader : public ObjectReader {
 public:
  CachingObjectReader(std::unique_ptr<ObjectReader> source_reader, std::shared_ptr<CacheManager> cache_manager);

  void SetMaxCacheSize(int64_t size) { max_cache_size_ = size; }
  int64_t MaxCacheSize() { return max_cache_size_; }

  /**
   * The following functions implement ObjectReader.
   */
  StorageError Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) override;
  StorageError ReadTail(size_t num_last_bytes, ByteBuffer* buffer) override;
  const ObjectStatus& GetStatus() override;
  StorageError Close() override;

 private:
  /**
   * The default buffer size is either 1/6 of the overall memory capacity, or a fixed amount if not running inside a
   * Lambda function.
   */
  constexpr static size_t kMaxBufferSizeDivisor = 6;
  constexpr static size_t kFallbackMaxBufferSize = 64_MB;

  StorageError ServeFromCache(const CacheableLocation& location, ByteBuffer* buffer);
  StorageError FillCache(const CacheableLocation& cacheable_location);
  StorageError FillCacheWithTail();
  StorageError ReadTailAndSetSize(size_t num_last_bytes, ByteBuffer* buffer);
  size_t TryResolveLastByte(size_t last_byte);
  void DeallocateCache();

  /**
   * Determines the maximum cache size based on environment variables in the host environment. This function is not
   * thread-safe.
   */
  static size_t DetermineMaxCacheSize();

  std::unique_ptr<ObjectReader> source_;
  std::shared_ptr<CacheManager> cache_manager_;

  /**
   * The current implementation has only one single buffer that is used to cache data.
   */
  std::vector<char> cache_;
  std::optional<CacheableLocation> buffered_location_;
  int64_t max_cache_size_;
  bool source_size_is_known_;
};

}  // namespace skyrise
