#pragma once

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

#include "abstract_provider.hpp"
#include "storage/backend/dynamodb_storage.hpp"
#include "testing/aws_test.hpp"
#include "utils/assert.hpp"

namespace skyrise {

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class TemporaryDynamoDbTable {
 public:
  explicit TemporaryDynamoDbTable(std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client)
      : client_(std::move(client)),
        table_name_("skyrise-test-table-" +
                    RandomString(kTableRandomSuffixLength, kCharacterSetLower + kCharacterSetDecimal)),
        error_(StorageErrorType::kNoError) {
    error_ = DynamoDbStorage::CreateTable(client_, table_name_);
    if (!error_) {
      storage_ = std::make_unique<DynamoDbStorage>(client_, table_name_);
    }
  }
  ~TemporaryDynamoDbTable() {
    if (!error_) {
      DynamoDbStorage::DeleteTable(client_, table_name_);
    }
  }

  StorageError GetError() { return error_; }
  DynamoDbStorage& GetStorage() { return *storage_; }

 private:
  static constexpr size_t kTableRandomSuffixLength = 8;

  const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client_;
  const std::string table_name_;
  StorageError error_;
  std::unique_ptr<DynamoDbStorage> storage_;
};

class DynamoDbTestResources {
  friend class DynamoDbStorageProvider;

 public:
  DynamoDbTestResources() : client_(std::make_shared<const Aws::DynamoDB::DynamoDBClient>()), table_(client_) {}

 protected:
  AwsApi aws_api_;
  std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client_;
  TemporaryDynamoDbTable table_;
};

class DynamoDbStorageProvider : public StorageProvider {
 public:
  Storage& GetStorage() override {
    if (storage_->table_.GetError()) {
      Fail("Could not create table for testing. Aborting test execution.");
    }
    return storage_->table_.GetStorage();
  }

  bool IsEventuallyConsistent() override { return false; }

  void SetUp() override { storage_ = std::make_unique<DynamoDbTestResources>(); }
  void TearDown() override { storage_.reset(nullptr); }

 private:
  std::unique_ptr<DynamoDbTestResources> storage_;
};

}  // namespace skyrise
