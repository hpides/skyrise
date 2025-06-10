#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

#include "../test/lib/testing/aws_test.hpp"
#include "abstract_provider.hpp"
#include "storage/backend/s3_storage.hpp"
#include "utils/assert.hpp"

namespace skyrise {

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class TemporaryS3Bucket {
 public:
  explicit TemporaryS3Bucket(std::shared_ptr<const Aws::S3::S3Client> client)
      : error_(StorageErrorType::kNoError),
        client_(std::move(client)),
        bucket_name_("skyrise-test-bucket-" +
                     RandomString(kBucketRandomSuffixLength, kCharacterSetLower + kCharacterSetDecimal)) {
    error_ = S3Storage::CreateBucket(client_, bucket_name_);
    if (!error_) {
      storage_ = std::make_unique<S3Storage>(client_, bucket_name_);
    }
  }
  ~TemporaryS3Bucket() {
    if (!error_) {
      S3Storage::DeleteBucket(client_, bucket_name_);
    }
  }

  StorageError GetError() { return error_; }
  S3Storage& GetStorage() { return *storage_; }

 private:
  static constexpr size_t kBucketRandomSuffixLength = 8;

  std::unique_ptr<S3Storage> storage_;
  StorageError error_;
  std::shared_ptr<const Aws::S3::S3Client> client_;
  const std::string bucket_name_;
};

class S3TestResources {
  friend class S3StorageProvider;

 public:
  S3TestResources() : client_(std::make_shared<const Aws::S3::S3Client>()), bucket_(client_) {}

 protected:
  AwsApi aws_api_;
  std::shared_ptr<const Aws::S3::S3Client> client_;
  TemporaryS3Bucket bucket_;
};

class S3StorageProvider : public StorageProvider {
 public:
  Storage& GetStorage() override {
    if (storage_->bucket_.GetError()) {
      Fail("Could not create bucket for testing. Aborting test execution.");
    }
    return storage_->bucket_.GetStorage();
  }

  bool IsEventuallyConsistent() override { return true; }

  void SetUp() override { storage_ = std::make_unique<S3TestResources>(); }
  void TearDown() override { storage_.reset(nullptr); }

 private:
  std::unique_ptr<S3TestResources> storage_;
};

}  // namespace skyrise
