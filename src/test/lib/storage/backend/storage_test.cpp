#include <functional>
#include <thread>

#include <gtest/gtest.h>
#include <unistd.h>

#include "abstract_provider.hpp"
#include "filesystem_provider.hpp"
#include "s3_provider.hpp"
#include "utils/assert.hpp"

namespace skyrise {

namespace {

std::function<void(const char* data, size_t n)> FillBufferLambda(std::vector<char>* buffer) {
  return [buffer](const char* data, size_t n) { buffer->insert(buffer->end(), data, data + n); };
}

}  // namespace

template <typename Provider>
class AwsBaseStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = &provider_.GetStorage();
    is_eventually_consistent_ = provider_.IsEventuallyConsistent();
  }

  static void SetUpTestSuite() { provider_.SetUp(); }

  static void TearDownTestSuite() { provider_.TearDown(); }

  void WaitForObjectStatus(const std::string& object_identifier, bool visible) {
    constexpr int kNumTries = 16;

    if (!this->is_eventually_consistent_) {
      return;
    }

    for (int i = 0; i < kNumTries; i++) {
      ObjectStatus status = this->storage_->GetStatus(object_identifier);
      const auto& error = status.GetError();
      const auto error_type = error.GetType();
      if ((visible && error_type == StorageErrorType::kNotFound) ||
          (!visible && error_type == StorageErrorType::kNoError)) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        continue;
      }
      if ((visible && !error) || (!visible && error_type == StorageErrorType::kNotFound)) {
        return;
      }
    }
    Fail("Unexpected status of object. Aborting tests.");
  }

  void WaitForObjectToBecomeVisible(const std::string& object_identifier) {
    this->WaitForObjectStatus(object_identifier, true);
  }

  void WaitForObjectToVanish(const std::string& object_identifier) {
    this->WaitForObjectStatus(object_identifier, false);
  }

  static Provider provider_;
  Storage* storage_ = nullptr;
  bool is_eventually_consistent_ = false;
};

template <typename Provider>
Provider AwsBaseStorageTest<Provider>::provider_;

using StorageProviderTypes = ::testing::Types<FilesystemStorageProvider, S3StorageProvider>;

TYPED_TEST_SUITE(AwsBaseStorageTest, StorageProviderTypes, );
// Trailing comma on purpose (https://github.com/google/googletest/issues/1419)

TYPED_TEST(AwsBaseStorageTest, CreateReadDeleteSmallObject) {
  static const std::string kFilename = "small.txt";
  static const std::string kFileContent = "abcd";
  constexpr size_t kFileSize = 4;

  // Create
  auto writer = this->storage_->OpenForWriting(kFilename);
  EXPECT_FALSE(writer->Write(kFileContent.c_str(), kFileSize));
  EXPECT_FALSE(writer->Close());

  this->WaitForObjectToBecomeVisible(kFilename);

  ObjectStatus status = this->storage_->GetStatus(kFilename);
  EXPECT_FALSE(status.GetError());
  EXPECT_EQ(status.GetSize(), 4);
  EXPECT_EQ(status.GetIdentifier(), kFilename);
  EXPECT_NE(status.GetChecksum(), "");
  EXPECT_NE(status.GetLastModifiedTimestamp(), static_cast<time_t>(0));

  // Read
  auto reader = this->storage_->OpenForReading(kFilename);
  std::vector<char> buffer;
  buffer.reserve(kFileSize);
  EXPECT_FALSE(reader->Read(0, ObjectReader::kLastByteInFile, FillBufferLambda(&buffer)));
  EXPECT_FALSE(reader->Close());

  EXPECT_EQ(buffer.size(), kFileSize);

  for (size_t i = 0; i < buffer.size(); i++) {
    EXPECT_EQ(buffer[i], kFileContent[i]);
  }

  // Read specific byte ranges
  buffer.clear();
  reader = this->storage_->OpenForReading(kFilename);
  auto compare_against = kFileContent.substr(1, 2);
  EXPECT_FALSE(reader->Read(1, 2, FillBufferLambda(&buffer)));
  EXPECT_FALSE(reader->Close());

  EXPECT_EQ(buffer.size(), 2);
  for (size_t i = 0; i < buffer.size(); i++) {
    EXPECT_EQ(buffer[i], compare_against[i]);
  }

  // Delete
  EXPECT_FALSE(this->storage_->Delete(kFilename));
  this->WaitForObjectToVanish(kFilename);
}

TYPED_TEST(AwsBaseStorageTest, CreateReadTailDeleteSmallObject) {
  // This test needs to be seperate from the former test to cover code paths that extract status information from
  // partial requests.

  static const std::string kFilename = "small.txt";
  static const std::string kFileContent = "abcd";
  constexpr size_t kFileSize = 4;

  // Create
  auto writer = this->storage_->OpenForWriting(kFilename);
  EXPECT_FALSE(writer->Write(kFileContent.c_str(), kFileSize));
  EXPECT_FALSE(writer->Close());

  this->WaitForObjectToBecomeVisible(kFilename);

  // ReadTail
  auto reader = this->storage_->OpenForReading(kFilename);
  std::vector<char> buffer;
  buffer.reserve(kFileSize);
  EXPECT_FALSE(reader->ReadTail(1, FillBufferLambda(&buffer)));

  EXPECT_EQ(buffer.size(), 1);
  EXPECT_EQ(buffer[0], 'd');

  EXPECT_FALSE(reader->GetStatus().GetError().IsError());
  EXPECT_EQ(reader->GetStatus().GetSize(), kFileSize);

  EXPECT_FALSE(reader->Close());

  // Delete
  EXPECT_FALSE(this->storage_->Delete(kFilename));
  this->WaitForObjectToVanish(kFilename);
}

TYPED_TEST(AwsBaseStorageTest, CreateReadDeleteBigObject) {
  constexpr size_t kChunkSize = 16_KB;
  constexpr size_t kTestFileSize = 31_MB;
  static const std::string kFilename{"big.txt"};
  std::vector<char> buffer(kChunkSize, 'x');

  // Create
  auto writer = this->storage_->OpenForWriting(kFilename);
  for (size_t written = 0; written < kTestFileSize; written += kChunkSize) {
    EXPECT_FALSE(writer->Write(buffer.data(), std::min(kTestFileSize - written, kChunkSize)));
  }
  EXPECT_FALSE(writer->Close());

  this->WaitForObjectToBecomeVisible(kFilename);

  // Read
  bool error = false;
  auto reader = this->storage_->OpenForReading(kFilename);
  EXPECT_FALSE(reader->Read(0, ObjectReader::kLastByteInFile, [&error](const char* data, size_t n) {
    for (size_t i = 0; i < n; i++) {
      if (data[i] != 'x') {
        error = true;
      }
    }
  }));
  EXPECT_FALSE(error);
  EXPECT_FALSE(reader->Close());

  // Delete
  EXPECT_FALSE(this->storage_->Delete(kFilename));
  this->WaitForObjectToVanish(kFilename);
}

TYPED_TEST(AwsBaseStorageTest, ListObjects) {
  static const std::string kFilename1{"file1.txt"};
  static const std::string kFilename2{"file2.txt"};
  static const std::string kFileContent1{"abcd"};
  static const std::string kFileContent2{"efghi"};
  constexpr size_t kFileSize1 = 4;
  constexpr size_t kFileSize2 = 5;

  // Create
  auto writer = this->storage_->OpenForWriting(kFilename1);
  EXPECT_FALSE(writer->Write(kFileContent1.c_str(), kFileSize1));
  EXPECT_FALSE(writer->Close());

  writer = this->storage_->OpenForWriting(kFilename2);
  EXPECT_FALSE(writer->Write(kFileContent2.c_str(), kFileSize2));
  EXPECT_FALSE(writer->Close());

  this->WaitForObjectToBecomeVisible(kFilename1);
  this->WaitForObjectToBecomeVisible(kFilename2);

  auto list_result = this->storage_->List();
  std::vector<ObjectStatus>& list = list_result.first;
  StorageError& error = list_result.second;

  EXPECT_FALSE(error);
  EXPECT_GE(list.size(), 2);

  bool found_file1 = false;
  bool found_file2 = false;

  for (const auto& status : list) {
    if (status.GetIdentifier() == kFilename1) {
      EXPECT_EQ(status.GetSize(), 4);
      found_file1 = true;
    } else if (status.GetIdentifier() == kFilename2) {
      EXPECT_EQ(status.GetSize(), 5);
      found_file2 = true;
    }
  }
  EXPECT_TRUE(found_file1);
  EXPECT_TRUE(found_file2);

  EXPECT_FALSE(this->storage_->Delete(kFilename1));
  EXPECT_FALSE(this->storage_->Delete(kFilename2));
  this->WaitForObjectToVanish(kFilename1);
  this->WaitForObjectToVanish(kFilename2);
}

}  // namespace skyrise
