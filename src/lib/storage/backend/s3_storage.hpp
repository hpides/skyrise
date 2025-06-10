#pragma once

#include <chrono>
#include <random>
#include <sstream>
#include <streambuf>
#include <string>
#include <utility>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "abstract_storage.hpp"
#include "stream.hpp"
#include "utils/literal.hpp"
#include "utils/string.hpp"

namespace skyrise {

StorageErrorType TranslateS3Error(const Aws::S3::S3Errors error);

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class S3MultipartUploader {
 public:
  S3MultipartUploader(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket, std::string object_id);
  ~S3MultipartUploader();
  bool IsInitialized() const { return is_initialized_; }
  void Initialize();
  StorageError Finalize();
  void UploadPartAsync(const std::shared_ptr<std::stringbuf>& buffer);
  StorageError CancelUpload();

 private:
  void SetError(const StorageError& error);

  std::shared_ptr<const Aws::S3::S3Client> client_;
  const std::string bucket_;
  const std::string object_id_;
  bool is_initialized_{false};
  int part_count_{1};
  std::string upload_id_;
  std::vector<Aws::S3::Model::CompletedPart> parts_;
  std::mutex multipart_outcome_mutex_;
  std::condition_variable multipart_upload_finished_condition_;
  int in_process_parts_count_{0};
  StorageError storage_error_ = StorageError::Success();
};

struct S3ObjectWriterStatistics {
  bool was_multipart_upload;
  size_t put_request_count;
  size_t bytes_transferred;
};

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class S3ObjectWriter : public ObjectWriter {
 public:
  S3ObjectWriter(std::shared_ptr<const Aws::S3::S3Client> client, const std::string& bucket,
                 const std::string& object_id);
  ~S3ObjectWriter() override;

  StorageError Write(const char* data, size_t length) override;
  StorageError Close() override;

  S3ObjectWriterStatistics GetRequestStatistics() { return statistics_; }

 private:
  static constexpr size_t kMultipartThresholdBytes = 16_MB;
  static constexpr size_t kMultipartSizeBytes = 16_MB;

  StorageError FinalizeUpload();
  StorageError SinglePutUpload();
  void UploadNextPartAsync();

  std::shared_ptr<const Aws::S3::S3Client> client_;
  const std::string bucket_;
  const std::string object_id_;
  std::vector<std::shared_ptr<std::stringbuf>> parts_;
  size_t current_part_size_bytes_{0};
  bool closed_{false};
  bool use_multipart_upload_{false};
  S3MultipartUploader multipart_uploader_;
  S3ObjectWriterStatistics statistics_{};
};

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class S3ObjectReader : public ObjectReader {
 public:
  S3ObjectReader(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket, std::string object_id);
  S3ObjectReader(const S3ObjectReader&) = delete;

  StorageError Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) override;
  StorageError ReadObjectAsync(
      const std::shared_ptr<ObjectBuffer>& buffer,
      std::optional<std::vector<std::pair<size_t, size_t>>> ranges_optional = std::nullopt) override;
  StorageError ReadTail(size_t num_last_bytes, ByteBuffer* buffer) override;
  const ObjectStatus& GetStatus() override;
  StorageError Close() override;

 private:
  static std::string GetRangeString(size_t first_byte, size_t last_byte);
  static std::string GetRangeStringForTail(size_t num_last_bytes);
  static size_t ParseContentLengthFromRange(const Aws::String& content_range);
  size_t GetObjectSize();

  Aws::S3::Model::GetObjectRequest CreateGetObjectRequest(ByteBuffer* buffer, const std::string& range = "");
  StorageError ProcessGetObjectRequest(const Aws::S3::Model::GetObjectRequest& request);
  StorageError ProcessGetObjectRequestsAsync(
      const std::shared_ptr<std::vector<Aws::S3::Model::GetObjectRequest>>& requests);

  std::shared_ptr<std::vector<Aws::S3::Model::GetObjectRequest>> BuildRequestsFromByteRanges(
      const std::vector<std::pair<size_t, size_t>>& ranges, const std::shared_ptr<ObjectBuffer>& buffer);

  std::shared_ptr<const Aws::S3::S3Client> client_;
  const std::string bucket_;
  const std::string object_id_;
  std::optional<size_t> object_size_;
  std::mutex mutex_;
  std::condition_variable condition_variable_;
};

class S3Storage : public Storage {
 public:
  S3Storage(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket);

  static StorageError CreateBucket(const std::shared_ptr<const Aws::S3::S3Client>& client, const std::string& bucket);
  static StorageError DeleteBucket(const std::shared_ptr<const Aws::S3::S3Client>& client, const std::string& bucket);

  std::unique_ptr<ObjectWriter> OpenForWriting(const std::string& object_identifier) override {
    return std::make_unique<S3ObjectWriter>(client_, bucket_, object_identifier);
  }

  std::unique_ptr<ObjectReader> OpenForReading(const std::string& object_identifier) override {
    return std::make_unique<S3ObjectReader>(client_, bucket_, object_identifier);
  }

  StorageError Delete(const std::string& object_identifier) override;
  std::pair<std::vector<ObjectStatus>, StorageError> List(const std::string& object_prefix) override;

 private:
  std::shared_ptr<const Aws::S3::S3Client> client_;
  const std::string bucket_;
};

}  // namespace skyrise
