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

class S3MultipartUploader {
 public:
  S3MultipartUploader(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket, std::string object_id);
  ~S3MultipartUploader();
  bool IsInitialized() const { return is_initialized_; }
  StorageError Initialize();
  StorageError Finalize();
  StorageError WriteChunk(std::stringbuf* buffer);
  StorageError CancelUpload();

 private:
  std::shared_ptr<const Aws::S3::S3Client> client_;
  const std::string bucket_;
  const std::string object_id_;
  bool is_initialized_;
  int part_count_;
  std::string upload_id_;
  std::vector<Aws::S3::Model::CompletedPart> parts_;
};

struct S3ObjectWriterStatistics {
  bool was_multipart_upload;
  size_t num_put_requests;
  size_t bytes_transferred;
};

class S3ObjectWriter : public ObjectWriter {
 public:
  S3ObjectWriter(std::shared_ptr<const Aws::S3::S3Client> client, const std::string& bucket,
                 const std::string& object_id);
  ~S3ObjectWriter() override;

  StorageError Write(const char* data, size_t length) override;
  StorageError Close() override;

  S3ObjectWriterStatistics GetRequestStatistics() { return statistics_; }

 private:
  static constexpr size_t kMultipartByteThreshold = 16_MB;
  static constexpr size_t kMultipartByteSize = 16_MB;

  StorageError FinalizeUpload();
  StorageError SinglePutUpload();
  StorageError MultipartUploadNext();

  std::shared_ptr<const Aws::S3::S3Client> client_;
  const std::string bucket_;
  const std::string object_id_;
  std::stringbuf buffer_;
  size_t bytes_written_;
  bool closed_;
  bool use_multipart_upload_;
  S3MultipartUploader uploader_;
  S3ObjectWriterStatistics statistics_{};
};

class S3ObjectReader : public ObjectReader {
 public:
  S3ObjectReader(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket, std::string object_id);
  S3ObjectReader(const S3ObjectReader&) = delete;

  StorageError Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) override;
  StorageError ReadTail(size_t num_last_bytes, ByteBuffer* buffer) override;
  const ObjectStatus& GetStatus() override;
  StorageError Close() override;

 private:
  static std::string GetRangeString(size_t first_byte, size_t last_byte);
  static std::string GetRangeStringForTail(size_t num_last_bytes);
  static size_t ParseContentLengthFromRange(const Aws::String& content_range);

  Aws::S3::Model::GetObjectRequest CreateGetObjectRequest(ByteBuffer* buffer, const std::string& range = "");
  StorageError ProcessGetObjectRequest(const Aws::S3::Model::GetObjectRequest& request);

  std::shared_ptr<const Aws::S3::S3Client> client_;
  const std::string bucket_;
  const std::string object_id_;
  DelegateStreamBuffer stream_;
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
