#include "storage/backend/caching_object_reader.hpp"

#include <cstdlib>
#include <set>

#include <gtest/gtest.h>

namespace skyrise {

namespace {

class LoggingReader : public ObjectReader {
 public:
  LoggingReader(size_t object_size)
      : status_("testing", 0, "checksum", object_size),
        requests_(std::make_shared<std::multiset<CacheableLocation>>()) {}

  StorageError Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) override {
    last_byte = (last_byte == ObjectReader::kLastByteInFile) ? status_.GetSize() - 1 : last_byte;
    const size_t length = last_byte - first_byte + 1;
    buffer->Resize(length);
    char c = static_cast<char>(first_byte % 256);
    for (size_t i = 0; i < length; ++i, ++c) {
      buffer->CharData()[i] = c;
    }
    requests_->emplace(CacheableLocation::WithFirstLastByteInclusive(first_byte, last_byte));
    return StorageError::Success();
  }

  StorageError ReadTail(size_t num_last_bytes, ByteBuffer* buffer) override {
    const size_t first_byte = std::max<int64_t>(0, status_.GetSize() - num_last_bytes);
    return Read(first_byte, status_.GetSize() - 1, buffer);
  }

  const ObjectStatus& GetStatus() override { return status_; }
  StorageError Close() override { return StorageError::Success(); }
  std::shared_ptr<std::multiset<CacheableLocation>>& GetAllRequests() { return requests_; }

 private:
  ObjectStatus status_;
  std::shared_ptr<std::multiset<CacheableLocation>> requests_;
};

}  // namespace

TEST(CachingObjectReaderTest, FlatRanges) {
  const auto locations = CacheManager()
                             .AddLocation(CacheableLocation::WithOffsetSize(0, 10))
                             .AddLocation(CacheableLocation::WithOffsetSize(20, 10))
                             .AddLocation(CacheableLocation::WithFirstLastByteInclusive(50, 51));

  const std::optional<CacheableLocation> on_cacheable =
      locations.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(5, 2), 10);
  EXPECT_TRUE(on_cacheable);
  EXPECT_EQ(on_cacheable->Size(), 10);
  EXPECT_EQ(on_cacheable->Offset(), 0);

  const std::optional<CacheableLocation> off_cacheable =
      locations.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(11, 2), 10);
  EXPECT_FALSE(off_cacheable);
}

TEST(CachingObjectReaderTest, NestedRanges) {
  const auto locations = CacheManager()
                             .AddLocation(CacheableLocation::WithOffsetSize(0, 100))
                             .AddLocation(CacheableLocation::WithOffsetSize(0, 10))
                             .AddLocation(CacheableLocation::WithOffsetSize(0, 20));

  const std::optional<CacheableLocation> cache10 =
      locations.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(1, 2), 10);
  EXPECT_TRUE(cache10);
  EXPECT_EQ(cache10->Size(), 10);
  EXPECT_EQ(cache10->Offset(), 0);

  const std::optional<CacheableLocation> cache20 =
      locations.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(1, 2), 20);
  EXPECT_TRUE(cache20);
  EXPECT_EQ(cache20->Size(), 20);
  EXPECT_EQ(cache20->Offset(), 0);

  const std::optional<CacheableLocation> cache100 =
      locations.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(1, 2), 100);
  EXPECT_TRUE(cache20);
  EXPECT_EQ(cache100->Size(), 100);
  EXPECT_EQ(cache100->Offset(), 0);
}

TEST(CachingObjectReaderTest, CacheableTail) {
  const auto locations = CacheManager().SetSourceSize(100).AddTail(10).SetSourceSize(100);
  // SetSourceSize(...) is called twice intentionally.

  const std::optional<CacheableLocation> on_cacheable =
      locations.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(90, 2), 10);
  EXPECT_TRUE(on_cacheable);
  EXPECT_EQ(on_cacheable->Size(), 10);
  EXPECT_EQ(on_cacheable->Offset(), 90);

  const std::optional<CacheableLocation> off_cacheable =
      locations.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(85, 10), 10);
  EXPECT_FALSE(off_cacheable);
}

TEST(CachingObjectReaderTest, AdjustedRanges) {
  const auto locations_sequential = CacheManager()
                                        .AddLocation(CacheableLocation::WithOffsetSize(0, 10))
                                        .SetAccessPattern(CacheAccessPattern::kSequential);
  const auto locations_random = CacheManager()
                                    .AddLocation(CacheableLocation::WithOffsetSize(0, 10))
                                    .SetAccessPattern(CacheAccessPattern::kRandom);

  // With sequential access the buffer will be extended to the right.
  const std::optional<CacheableLocation> location1 =
      locations_sequential.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(4, 2), 5);
  EXPECT_TRUE(location1);
  EXPECT_EQ(location1->Offset(), 4);
  EXPECT_EQ(location1->Size(), 5);

  // The cache will never be extended across the boundaries of a cacheable location.
  const std::optional<CacheableLocation> location2 =
      locations_sequential.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(8, 2), 5);
  EXPECT_TRUE(location2);
  EXPECT_EQ(location2->Offset(), 8);
  EXPECT_EQ(location2->Size(), 2);

  // With random access we try to center align the cached region.
  const std::optional<CacheableLocation> location3 =
      locations_random.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(4, 2), 5);
  EXPECT_TRUE(location3);
  EXPECT_EQ(location3->Offset(), 3);
  EXPECT_EQ(location3->Size(), 5);

  // Again, we will never break boundaries.
  const std::optional<CacheableLocation> location4 =
      locations_random.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(8, 2), 5);
  EXPECT_TRUE(location4);
  EXPECT_EQ(location4->Offset(), 5);
  EXPECT_EQ(location4->Size(), 5);

  const std::optional<CacheableLocation> location5 =
      locations_random.CacheableLocationIncluding(CacheableLocation::WithOffsetSize(1, 2), 6);
  EXPECT_TRUE(location5);
  EXPECT_EQ(location5->Offset(), 0);
  EXPECT_EQ(location5->Size(), 6);
}

TEST(CachingObjectReaderTest, ObjectReaderSequentialCaching) {
  constexpr size_t kVirtualObjectSize = 200;

  auto reader = std::make_unique<LoggingReader>(kVirtualObjectSize);
  auto requests = reader->GetAllRequests();
  auto locations = std::make_shared<CacheManager>();
  locations->AddLocation(CacheableLocation::WithOffsetSize(0, 100));
  locations->AddLocation(CacheableLocation::WithOffsetSize(100, 100));
  locations->SetAccessPattern(CacheAccessPattern::kSequential);
  auto caching_reader = std::make_unique<CachingObjectReader>(std::move(reader), locations);
  caching_reader->SetMaxCacheSize(50);

  ByteBuffer tmp_data_destination;
  caching_reader->Read(5, 9, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 5);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 5);
  EXPECT_EQ(tmp_data_destination.CharData()[4], 9);
  caching_reader->Read(20, 39, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 20);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 20);
  EXPECT_EQ(tmp_data_destination.CharData()[19], 39);

  EXPECT_EQ(requests->size(), 1);
  EXPECT_NE(requests->find(CacheableLocation::WithFirstLastByteInclusive(5, 54)), requests->end());
  caching_reader->Close();
}

TEST(CachingObjectReaderTest, ObjectReaderRandomCaching) {
  constexpr size_t kVirtualObjectSize = 200;

  auto reader = std::make_unique<LoggingReader>(kVirtualObjectSize);
  auto requests = reader->GetAllRequests();
  auto locations = std::make_shared<CacheManager>();
  locations->AddLocation(CacheableLocation::WithOffsetSize(0, 100));
  locations->AddLocation(CacheableLocation::WithOffsetSize(100, 100));
  locations->SetAccessPattern(CacheAccessPattern::kRandom);
  auto caching_reader = std::make_unique<CachingObjectReader>(std::move(reader), locations);
  caching_reader->SetMaxCacheSize(50);

  ByteBuffer tmp_data_destination;
  caching_reader->Read(5, 9, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 5);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 5);
  EXPECT_EQ(tmp_data_destination.CharData()[4], 9);
  caching_reader->Read(0, 5, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 6);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 0);
  EXPECT_EQ(tmp_data_destination.CharData()[5], 5);

  EXPECT_EQ(requests->size(), 1);
  EXPECT_NE(requests->find(CacheableLocation::WithFirstLastByteInclusive(0, 49)), requests->end());
  caching_reader->Close();
}

TEST(CachingObjectReaderTest, ObjectReaderTailCaching) {
  constexpr size_t kVirtualObjectSize = 200;

  auto reader = std::make_unique<LoggingReader>(kVirtualObjectSize);
  auto requests = reader->GetAllRequests();
  auto locations = std::make_shared<CacheManager>();
  locations->AddTail(50);
  auto caching_reader = std::make_unique<CachingObjectReader>(std::move(reader), locations);
  caching_reader->SetMaxCacheSize(50);

  ByteBuffer tmp_data_destination;
  caching_reader->ReadTail(10, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], static_cast<char>(190));
  EXPECT_EQ(tmp_data_destination.CharData()[9], static_cast<char>(199));
  caching_reader->Read(165, 199, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 35);
  EXPECT_EQ(tmp_data_destination.CharData()[0], static_cast<char>(165));
  EXPECT_EQ(tmp_data_destination.CharData()[34], static_cast<char>(199));

  EXPECT_EQ(requests->size(), 1);
  EXPECT_NE(requests->find(CacheableLocation::WithFirstLastByteInclusive(150, 199)), requests->end());
  caching_reader->Close();
}

TEST(CachingObjectReaderTest, ObjectReaderLargerCacheThanFile) {
  constexpr size_t kVirtualObjectSize = 100;

  auto reader = std::make_unique<LoggingReader>(kVirtualObjectSize);
  auto requests = reader->GetAllRequests();
  auto locations = std::make_shared<CacheManager>();
  locations->AddTail(1024);
  auto caching_reader = std::make_unique<CachingObjectReader>(std::move(reader), locations);
  caching_reader->SetMaxCacheSize(1024);

  ByteBuffer tmp_data_destination;
  caching_reader->ReadTail(10, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 90);
  EXPECT_EQ(tmp_data_destination.CharData()[9], 99);
  caching_reader->Read(0, 9, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 0);
  EXPECT_EQ(tmp_data_destination.CharData()[9], 9);

  EXPECT_EQ(requests->size(), 1);
  EXPECT_NE(requests->find(CacheableLocation::WithFirstLastByteInclusive(0, 99)), requests->end());
  caching_reader->Close();
}

TEST(CachingObjectReaderTest, ObjectReaderRequestsLargerThanCache) {
  constexpr size_t kVirtualObjectSize = 100;

  auto reader = std::make_unique<LoggingReader>(kVirtualObjectSize);
  auto requests = reader->GetAllRequests();
  auto locations = std::make_shared<CacheManager>();
  locations->AddLocation(CacheableLocation::WithOffsetSize(0, 50));
  locations->AddLocation(CacheableLocation::WithOffsetSize(50, 100));
  auto caching_reader = std::make_unique<CachingObjectReader>(std::move(reader), locations);
  caching_reader->SetMaxCacheSize(5);

  ByteBuffer tmp_data_destination;
  caching_reader->ReadTail(10, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 90);
  EXPECT_EQ(tmp_data_destination.CharData()[9], 99);

  caching_reader->Read(0, 9, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 0);
  EXPECT_EQ(tmp_data_destination.CharData()[9], 9);

  caching_reader->Read(0, 9, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 0);
  EXPECT_EQ(tmp_data_destination.CharData()[9], 9);

  caching_reader->Read(10, 19, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 10);
  EXPECT_EQ(tmp_data_destination.CharData()[9], 19);

  EXPECT_EQ(requests->size(), 4);
  caching_reader->Close();
}

TEST(CachingObjectReaderTest, ObjectReaderRequestsLargerThanTailCache) {
  constexpr size_t kVirtualObjectSize = 100;

  auto reader = std::make_unique<LoggingReader>(kVirtualObjectSize);
  auto requests = reader->GetAllRequests();
  auto locations = std::make_shared<CacheManager>();
  locations->AddTail(5);
  auto caching_reader = std::make_unique<CachingObjectReader>(std::move(reader), locations);
  caching_reader->SetMaxCacheSize(25);

  ByteBuffer tmp_data_destination;
  caching_reader->ReadTail(10, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 90);
  EXPECT_EQ(tmp_data_destination.CharData()[9], 99);

  caching_reader->ReadTail(10, &tmp_data_destination);
  EXPECT_EQ(tmp_data_destination.Size(), 10);
  EXPECT_EQ(tmp_data_destination.CharData()[0], 90);
  EXPECT_EQ(tmp_data_destination.CharData()[9], 99);

  EXPECT_EQ(requests->size(), 2);
  caching_reader->Close();
}

TEST(CachingObjectReaderTest, ObjectReaderDefaultCacheSize) {
  auto locations = std::make_shared<CacheManager>();
  auto reader1 = std::make_unique<LoggingReader>(1);
  auto reader2 = std::make_unique<LoggingReader>(1);
  auto reader3 = std::make_unique<LoggingReader>(1);

  ::setenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128", 1);
  const int64_t size_low = CachingObjectReader(std::move(reader1), locations).MaxCacheSize();

  ::setenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "10240", 1);
  const int64_t size_high = CachingObjectReader(std::move(reader2), locations).MaxCacheSize();

  ::unsetenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
  const int64_t size_fallback = CachingObjectReader(std::move(reader3), locations).MaxCacheSize();

  EXPECT_LT(size_low, 128_MB);
  EXPECT_LT(size_high, 10240_MB);
  EXPECT_GT(size_high, size_low);
  EXPECT_GT(size_fallback, size_low);
}

TEST(CachingObjectReaderTest, ObjectReaderReadAll) {
  constexpr size_t kVirtualObjectSize = 100;

  auto reader = std::make_unique<LoggingReader>(kVirtualObjectSize);
  auto requests = reader->GetAllRequests();

  auto manager = std::make_shared<CacheManager>();
  manager->AddLocation(CacheableLocation::WithOffsetSize(0, kVirtualObjectSize));
  manager->SetSourceSize(kVirtualObjectSize);

  auto caching_reader = std::make_unique<CachingObjectReader>(std::move(reader), manager);
  caching_reader->SetMaxCacheSize(kVirtualObjectSize);

  ByteBuffer tmp_data_destination;
  auto error = caching_reader->Read(0, ObjectReader::kLastByteInFile, &tmp_data_destination);
  EXPECT_FALSE(error);
  EXPECT_EQ(tmp_data_destination.Size(), kVirtualObjectSize);

  error = caching_reader->Read(10, ObjectReader::kLastByteInFile, &tmp_data_destination);
  EXPECT_FALSE(error);
  EXPECT_EQ(tmp_data_destination.Size(), kVirtualObjectSize - 10);

  EXPECT_EQ(requests->size(), 2);
  caching_reader->Close();
}

}  // namespace skyrise
