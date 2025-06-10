#include <functional>
#include <thread>

#include <gtest/gtest.h>
#include <unistd.h>

#include "abstract_provider.hpp"
#include "configuration.hpp"
#include "constants.hpp"
#include "dynamodb_provider.hpp"
#include "filesystem_provider.hpp"
#include "s3_provider.hpp"
#include "utils/assert.hpp"

namespace skyrise {

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

    for (int i = 0; i < kNumTries; ++i) {
      const ObjectStatus status = this->storage_->GetStatus(object_identifier);
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

using StorageProviderTypes = ::testing::Types<DynamoDbStorageProvider, FilesystemStorageProvider, S3StorageProvider>;

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

  const ObjectStatus status = this->storage_->GetStatus(kFilename);
  EXPECT_FALSE(status.GetError());
  EXPECT_EQ(status.GetSize(), 4);
  EXPECT_EQ(status.GetIdentifier(), kFilename);
  EXPECT_NE(status.GetChecksum(), "");
  EXPECT_NE(status.GetLastModifiedTimestamp(), static_cast<time_t>(0));

  // Read
  auto reader = this->storage_->OpenForReading(kFilename);
  ByteBuffer buffer(kFileSize);
  EXPECT_FALSE(reader->Read(0, ObjectReader::kLastByteInFile, &buffer));
  EXPECT_FALSE(reader->Close());

  EXPECT_EQ(buffer.Size(), kFileSize);

  for (size_t i = 0; i < buffer.Size(); ++i) {
    EXPECT_EQ(buffer.CharData()[i], kFileContent[i]);
  }

  // Read specific byte ranges
  buffer.Resize(0);
  reader = this->storage_->OpenForReading(kFilename);
  auto compare_against = kFileContent.substr(1, 2);
  EXPECT_FALSE(reader->Read(1, 2, &buffer));
  EXPECT_FALSE(reader->Close());

  EXPECT_EQ(buffer.Size(), 2);
  for (size_t i = 0; i < buffer.Size(); ++i) {
    EXPECT_EQ(buffer.CharData()[i], compare_against[i]);
  }

  // Delete
  EXPECT_FALSE(this->storage_->Delete(kFilename));
  this->WaitForObjectToVanish(kFilename);
}

TYPED_TEST(AwsBaseStorageTest, CreateReadTailDeleteSmallObject) {
  // This test needs to be separate from the former test to cover code paths that extract status information from
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
  ByteBuffer buffer;
  buffer.Resize(kFileSize);
  EXPECT_FALSE(reader->ReadTail(1, &buffer));

  EXPECT_EQ(buffer.Size(), 1);
  EXPECT_EQ(buffer.CharData()[0], 'd');

  EXPECT_FALSE(reader->GetStatus().GetError().IsError());
  EXPECT_EQ(reader->GetStatus().GetSize(), kFileSize);

  EXPECT_FALSE(reader->Close());

  // Delete
  EXPECT_FALSE(this->storage_->Delete(kFilename));
  this->WaitForObjectToVanish(kFilename);
}

TYPED_TEST(AwsBaseStorageTest, DISABLED_CreateReadDeleteBigObject) {
  constexpr size_t kChunkSize = 16_KB;
  size_t test_file_size = 31_MB;

  // DynamoDB's maximum item size is 400KB, including data and metadata.
  if (this->storage_ != nullptr) {
    if (dynamic_cast<DynamoDbStorage*>(this->storage_) != nullptr) {
      test_file_size = kDynamoDbMaxItemSizeBytes - kDynamoDbStorageMetadataSizeBytes;
    }
  } else {
    FAIL() << "No storage backend provided.";
  }

  static const std::string kFilename{"big.txt"};
  std::vector<char> buffer(kChunkSize, 'x');

  // Create
  auto writer = this->storage_->OpenForWriting(kFilename);
  for (size_t written = 0; written < test_file_size; written += kChunkSize) {
    EXPECT_FALSE(writer->Write(buffer.data(), std::min(test_file_size - written, kChunkSize)));
  }
  EXPECT_FALSE(writer->Close());

  this->WaitForObjectToBecomeVisible(kFilename);

  // Read
  auto reader = this->storage_->OpenForReading(kFilename);
  ByteBuffer read_buffer;
  EXPECT_FALSE(reader->Read(0, ObjectReader::kLastByteInFile, &read_buffer));
  EXPECT_EQ(reader->GetStatus().GetSize(), test_file_size);
  EXPECT_TRUE(std::find_if(read_buffer.CharData(), read_buffer.CharData() + read_buffer.Size(),
                           [](char x) { return x != 'x'; }) == read_buffer.CharData() + read_buffer.Size());
  EXPECT_FALSE(reader->Close());

  // Delete
  StorageError storage_error = this->storage_->Delete(kFilename);
  EXPECT_FALSE(storage_error.IsError()) << storage_error.GetMessage();
  this->WaitForObjectToVanish(kFilename);
}

TYPED_TEST(AwsBaseStorageTest, CreateReadAsyncDeleteBigObject) {
  constexpr size_t kChunkSize = 16_KB;
  size_t test_file_size = 31_MB;

  static const std::string kFilename{"big.txt"};
  std::vector<char> buffer(kChunkSize, 'x');

  // DynamoDB's maximum item size is 400KB, including data and metadata.
  if (this->storage_ != nullptr) {
    if (dynamic_cast<DynamoDbStorage*>(this->storage_) != nullptr) {
      test_file_size = kDynamoDbMaxItemSizeBytes - kDynamoDbStorageMetadataSizeBytes;
    }
  } else {
    FAIL() << "No storage backend provided.";
  }

  // Create
  auto writer = this->storage_->OpenForWriting(kFilename);
  for (size_t written = 0; written < test_file_size; written += kChunkSize) {
    EXPECT_FALSE(writer->Write(buffer.data(), std::min(test_file_size - written, kChunkSize)));
  }
  EXPECT_FALSE(writer->Close());

  this->WaitForObjectToBecomeVisible(kFilename);

  auto reader = this->storage_->OpenForReading(kFilename);
  auto object_buffer = std::make_shared<ObjectBuffer>();

  // Read
  if (this->storage_ != nullptr) {
    if (dynamic_cast<DynamoDbStorage*>(this->storage_) != nullptr ||
        dynamic_cast<FilesystemStorage*>(this->storage_) != nullptr) {
      // TODO(tobodner): Provide an async read implementation for DynamoDbStorage and FilesystemStorage.
      EXPECT_ANY_THROW(reader->ReadObjectAsync(object_buffer, std::nullopt));
    } else {
      EXPECT_FALSE(reader->ReadObjectAsync(object_buffer, std::nullopt).IsError());
      auto byte_buffer = object_buffer->Read(0, test_file_size);
      EXPECT_TRUE(std::find_if(byte_buffer->CharData(), byte_buffer->CharData() + byte_buffer->Size(),
                               [](char x) { return x != 'x'; }) == byte_buffer->CharData() + byte_buffer->Size());
      EXPECT_FALSE(reader->Close());
    }
  }

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
  const std::vector<ObjectStatus>& list = list_result.first;
  const StorageError& error = list_result.second;

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
