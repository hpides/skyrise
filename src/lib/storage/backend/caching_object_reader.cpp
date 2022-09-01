#include "caching_object_reader.hpp"

#include <cmath>
#include <cstdlib>

namespace skyrise {

CacheableLocation CacheableLocation::WithOffsetSize(int64_t offset, int64_t size) {
  CacheableLocation location;
  location.offset_ = offset;
  location.size_ = size;
  location.last_byte_ = offset + size - 1;

  return location;
}

CacheableLocation CacheableLocation::WithFirstLastByteInclusive(int64_t first_byte, int64_t last_byte) {
  CacheableLocation location;
  location.offset_ = first_byte;
  location.last_byte_ = last_byte;
  location.size_ = last_byte - first_byte + 1;

  return location;
}

bool CacheableLocation::operator<(const CacheableLocation& other) const { return Offset() < other.Offset(); }

bool CacheableLocation::Includes(const CacheableLocation& other) const {
  return FirstByte() <= other.FirstByte() && LastByteInclusive() >= other.LastByteInclusive();
}

CacheManager& CacheManager::AddTail(int64_t min_bytes) {
  tail_cache_ = min_bytes;
  AddTailIfPossible();
  return *this;
}

CacheManager& CacheManager::SetSourceSize(int64_t size) {
  source_size_ = size;
  AddTailIfPossible();
  return *this;
}

CacheManager& CacheManager::AddLocation(const CacheableLocation& location) {
  locations_.insert(location);
  return *this;
}

CacheManager& CacheManager::SetAccessPattern(CacheAccessPattern access_pattern) {
  access_pattern_ = access_pattern;
  return *this;
}

void CacheManager::AddTailIfPossible() {
  if (has_tail_) {
    return;
  }

  if (tail_cache_ > 0 && source_size_ > 0) {
    tail_cache_ = std::min(tail_cache_, source_size_);
    AddLocation(CacheableLocation::WithOffsetSize(source_size_ - tail_cache_, tail_cache_));
    has_tail_ = true;
  }
}

/**
 *  There are two strategies to adjust the requested location.
 *  Legend:
 *   -) Cacheable location
 *   *) Requested location
 *   +) Cached location
 *
 *  Given desired_size = 6,
 *   ExtendedLocation:  -----**++++
 *   MidpointLocation:  ---++**++--
 */
CacheableLocation CacheManager::ExtendedLocation(const CacheableLocation& requested_location,
                                                 const CacheableLocation& cached_location, int64_t desired_size) {
  return CacheableLocation::WithFirstLastByteInclusive(
      requested_location.FirstByte(),
      std::min<int64_t>(requested_location.FirstByte() + desired_size - 1, cached_location.LastByteInclusive()));
}

CacheableLocation CacheManager::MidpointLocation(const CacheableLocation& requested_location,
                                                 const CacheableLocation& cached_location, int64_t desired_size) {
  int64_t unused_bytes = desired_size - requested_location.Size();
  int64_t start_byte =
      std::max<int64_t>(requested_location.FirstByte() - (unused_bytes / 2), cached_location.FirstByte());

  unused_bytes = desired_size - (requested_location.LastByteExclusive() - start_byte);
  const int64_t last_byte_inclusive =
      std::min<int64_t>(requested_location.LastByteInclusive() + unused_bytes, cached_location.LastByteInclusive());

  unused_bytes = desired_size - (last_byte_inclusive - start_byte + 1);
  if (unused_bytes > 0) {
    start_byte = std::max<int64_t>(start_byte - unused_bytes, cached_location.FirstByte());
  }

  return CacheableLocation::WithFirstLastByteInclusive(start_byte, last_byte_inclusive);
}

CacheableLocation CacheManager::AdjustedLocation(const CacheableLocation& requested_location,
                                                 const CacheableLocation& cached_location, int64_t desired_size) const {
  // We know that requested_location.Size() <= desired_size < cached_location.Size().
  switch (access_pattern_) {
    case CacheAccessPattern::kSequential:
      return ExtendedLocation(requested_location, cached_location, desired_size);
    case CacheAccessPattern::kRandom:
      return MidpointLocation(requested_location, cached_location, desired_size);
  }

  throw std::logic_error("Unreachable code.");
}

std::optional<CacheableLocation> CacheManager::CacheableLocationIncluding(const CacheableLocation& location,
                                                                          int64_t desired_size) const {
  // All locations with offset <= location.Offset() could work.
  auto iterator = locations_.upper_bound(location);
  if (iterator == locations_.begin()) {
    return std::nullopt;
  }

  // We search for the CacheableLocation where its size has the minimal differences to the desired cache size.
  std::optional<CacheableLocation> cacheable_location;
  int64_t min_difference_to_desired_size = 0;

  do {
    --iterator;
    if (iterator->Includes(location)) {
      const int64_t difference_to_desired_size = std::abs(iterator->Size() - desired_size);
      if (!cacheable_location || difference_to_desired_size < min_difference_to_desired_size) {
        cacheable_location = *iterator;
        min_difference_to_desired_size = difference_to_desired_size;
      }
    }
  } while (iterator != locations_.begin());

  if (!cacheable_location) {
    return std::nullopt;
  }

  if (cacheable_location->Size() > desired_size) {
    // If the cacheable locations is larger than the desired size we need to adjust (i.e. shrink) the cacheable
    // location.
    return AdjustedLocation(location, *cacheable_location, desired_size);
  }

  return cacheable_location;
}

CachingObjectReader::CachingObjectReader(std::unique_ptr<ObjectReader> source_reader,
                                         std::shared_ptr<CacheManager> cache_manager)
    : source_(std::move(source_reader)), cache_manager_(std::move(cache_manager)), source_size_is_known_(false) {
  max_cache_size_ = DetermineMaxCacheSize();
}

size_t CachingObjectReader::DetermineMaxCacheSize() {
  const char* memory_mb_string = std::getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
  if (memory_mb_string == nullptr || memory_mb_string[0] == '\0') {
    return CachingObjectReader::kFallbackMaxBufferSize;
  }

  long memory_mb_number = std::atol(memory_mb_string) * 1_MB;
  if (memory_mb_number == 0) {
    return CachingObjectReader::kFallbackMaxBufferSize;
  }

  return memory_mb_number / kMaxBufferSizeDivisor;
}

const ObjectStatus& CachingObjectReader::GetStatus() { return source_->GetStatus(); }

size_t CachingObjectReader::TryResolveLastByte(size_t last_byte) {
  if (last_byte != ObjectReader::kLastByteInFile) {
    return last_byte;
  }

  if (source_size_is_known_) {
    return source_->GetStatus().GetSize() - 1;
  }

  return last_byte;
}

StorageError CachingObjectReader::Read(size_t first_byte, size_t last_byte, std::vector<char>* buffer) {
  const size_t resolved_last_byte = TryResolveLastByte(last_byte);

  // If we do not know the size of the request, nothing will be cached.
  if (resolved_last_byte == ObjectReader::kLastByteInFile) {
    return source_->Read(first_byte, resolved_last_byte, buffer);
  }

  const auto requested_location = CacheableLocation::WithFirstLastByteInclusive(first_byte, last_byte);

  // If more bytes are requested than the cache can hold, nothing will be cached. We do not assume that data is read
  // twice. Therefore the call is forwarded to the source reader.
  if (requested_location.Size() > max_cache_size_) {
    source_size_is_known_ = true;
    return source_->Read(first_byte, last_byte, buffer);
  }

  if (buffered_location_ && buffered_location_->Includes(requested_location)) {
    return ServeFromCache(requested_location, buffer);
  }

  // Try to find a cacheable location that includes the requested location.
  std::optional<CacheableLocation> cacheable_location =
      cache_manager_->CacheableLocationIncluding(requested_location, max_cache_size_);
  if (!cacheable_location) {
    // If no such location exists, the call is forwarded. With the first read-call to the source, its size is known.
    source_size_is_known_ = true;
    return source_->Read(first_byte, last_byte, buffer);
  }

  const StorageError error = FillCache(*cacheable_location);
  if (error) {
    return error;
  }

  return ServeFromCache(requested_location, buffer);
}

StorageError CachingObjectReader::FillCache(const CacheableLocation& cacheable_location) {
  buffered_location_.reset();

  const StorageError error =
      source_->Read(cacheable_location.FirstByte(), cacheable_location.LastByteInclusive(), &cache_);
  if (error) {
    return error;
  }

  source_size_is_known_ = true;
  buffered_location_.emplace(CacheableLocation::WithOffsetSize(cacheable_location.FirstByte(), cache_.size()));

  return StorageError::Success();
}

StorageError CachingObjectReader::ServeFromCache(const CacheableLocation& location, std::vector<char>* buffer) {
  buffer->reserve(location.Size());
  buffer->clear();

  auto begin = cache_.cbegin();
  begin += location.FirstByte() - buffered_location_->FirstByte();
  buffer->insert(buffer->end(), begin, begin + location.Size());

  return StorageError::Success();
}

void CachingObjectReader::DeallocateCache() {
  cache_.clear();
  cache_.shrink_to_fit();
  buffered_location_.reset();
}

StorageError CachingObjectReader::Close() {
  DeallocateCache();
  return source_->Close();
}

StorageError CachingObjectReader::ReadTailAndSetSize(size_t num_last_bytes, std::vector<char>* buffer) {
  source_size_is_known_ = true;
  const StorageError error = source_->ReadTail(num_last_bytes, buffer);
  if (!error) {
    cache_manager_->SetSourceSize(source_->GetStatus().GetSize());
  }
  return error;
}

StorageError CachingObjectReader::ReadTail(size_t num_last_bytes, std::vector<char>* buffer) {
  // If more bytes are requested than the cache can hold, nothing will be cached.
  if (num_last_bytes > static_cast<size_t>(max_cache_size_) ||
      num_last_bytes > static_cast<size_t>(cache_manager_->Tail())) {
    return ReadTailAndSetSize(num_last_bytes, buffer);
  }

  if (!source_size_is_known_) {
    const StorageError error = FillCacheWithTail();
    if (error) {
      return error;
    }
  }

  const ObjectStatus& status = source_->GetStatus();
  size_t first_byte = std::max<int64_t>(0, status.GetSize() - num_last_bytes);
  return Read(first_byte, status.GetSize() - 1, buffer);
}

StorageError CachingObjectReader::FillCacheWithTail() {
  const size_t request_size = std::min<int64_t>(cache_manager_->Tail(), max_cache_size_);
  const StorageError error = source_->ReadTail(request_size, &cache_);
  if (error) {
    return error;
  }

  const size_t source_size = source_->GetStatus().GetSize();
  cache_manager_->SetSourceSize(source_size);
  source_size_is_known_ = true;
  buffered_location_.emplace(
      CacheableLocation::WithFirstLastByteInclusive(source_size - cache_.size(), source_size - 1));

  return StorageError::Success();
}

}  // namespace skyrise
