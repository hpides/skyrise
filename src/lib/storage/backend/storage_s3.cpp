#include "storage_s3.hpp"

#include "utils/assert.hpp"

namespace skyrise {

namespace {

time_t ConvertAwsDateTime(const Aws::Utils::DateTime aws_datetime) {
  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::milliseconds(aws_datetime.Millis()));
  return seconds.count();
}

}  // namespace

StorageErrorType TranslateS3Error(const Aws::S3::S3Errors error) {
  switch (error) {
    case Aws::S3::S3Errors::INCOMPLETE_SIGNATURE:
    case Aws::S3::S3Errors::INVALID_ACTION:
    case Aws::S3::S3Errors::INVALID_PARAMETER_COMBINATION:
    case Aws::S3::S3Errors::INVALID_PARAMETER_VALUE:
    case Aws::S3::S3Errors::INVALID_QUERY_PARAMETER:
    case Aws::S3::S3Errors::INVALID_SIGNATURE:
    case Aws::S3::S3Errors::MALFORMED_QUERY_STRING:
    case Aws::S3::S3Errors::MISSING_ACTION:
    case Aws::S3::S3Errors::MISSING_PARAMETER:
    case Aws::S3::S3Errors::OPT_IN_REQUIRED:
    case Aws::S3::S3Errors::REQUEST_EXPIRED:
    case Aws::S3::S3Errors::REQUEST_TIME_TOO_SKEWED:
      return StorageErrorType::kInvalidArgument;

    case Aws::S3::S3Errors::INTERNAL_FAILURE:
    case Aws::S3::S3Errors::SERVICE_UNAVAILABLE:
      return StorageErrorType::kInternalError;

    case Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS:
      return StorageErrorType::kAlreadyExist;

    case Aws::S3::S3Errors::ACCESS_DENIED:
    case Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU:
    case Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID:
    case Aws::S3::S3Errors::INVALID_CLIENT_TOKEN_ID:
    case Aws::S3::S3Errors::MISSING_AUTHENTICATION_TOKEN:
    case Aws::S3::S3Errors::SIGNATURE_DOES_NOT_MATCH:
    case Aws::S3::S3Errors::UNRECOGNIZED_CLIENT:
    case Aws::S3::S3Errors::VALIDATION:
      return StorageErrorType::kPermissionDenied;

    case Aws::S3::S3Errors::RESOURCE_NOT_FOUND:
      return StorageErrorType::kNotFound;

    case Aws::S3::S3Errors::SLOW_DOWN:
    case Aws::S3::S3Errors::THROTTLING:
      return StorageErrorType::kTemporary;

    case Aws::S3::S3Errors::NETWORK_CONNECTION:
    case Aws::S3::S3Errors::REQUEST_TIMEOUT:
      return StorageErrorType::kIOError;

    case Aws::S3::S3Errors::NO_SUCH_BUCKET:
    case Aws::S3::S3Errors::NO_SUCH_KEY:
    case Aws::S3::S3Errors::NO_SUCH_UPLOAD:
      return StorageErrorType::kNotFound;
    default:
      return StorageErrorType::kUnknown;
  }
}

template <class AwsOutcomeClass>
StorageError GetErrorFromOutcome(const AwsOutcomeClass& outcome) {
  const auto& error = outcome.GetError();
  return StorageError(TranslateS3Error(error.GetErrorType()), error.GetMessage());
}

S3ObjectWriter::S3ObjectWriter(std::shared_ptr<const Aws::S3::S3Client> client, const std::string& bucket,
                               const std::string& object_id)
    : client_(std::move(client)),
      bucket_(bucket),
      object_id_(object_id),
      bytes_written_(0),
      closed_(false),
      use_multipart_upload_(false),
      uploader_(client_, bucket, object_id) {}

S3ObjectWriter::~S3ObjectWriter() { FinalizeUpload(); }

StorageError S3ObjectWriter::Write(const char* data, size_t length) {
  if (closed_) {
    return StorageError(StorageErrorType::kInvalidState);
  }

  if (length == 0) {
    return StorageError::Success();
  }

  size_t new_buffer_size = bytes_written_ + length;

  if (!use_multipart_upload_ && new_buffer_size > kMultipartByteThreshold) {
    use_multipart_upload_ = true;
    statistics_.was_multipart_upload = true;
  }

  if (use_multipart_upload_ && new_buffer_size > kMultipartByteSize) {
    while (new_buffer_size > kMultipartByteSize) {
      size_t left_to_write = kMultipartByteSize - bytes_written_;
      if (left_to_write > 0) {
        buffer_.sputn(data, left_to_write);
        length -= left_to_write;
        data += left_to_write;
      }
      StorageError err = MultipartUploadNext();
      if (err) {
        return err;
      }
      new_buffer_size = bytes_written_ + length;
    }
  }

  buffer_.sputn(data, length);
  bytes_written_ += length;

  return StorageError::Success();
}

StorageError S3ObjectWriter::Close() { return FinalizeUpload(); }

StorageError S3ObjectWriter::FinalizeUpload() {
  if (closed_) {
    return StorageError::Success();
  }

  closed_ = true;

  if (!use_multipart_upload_) {
    return SinglePutUpload();
  }

  if (bytes_written_ > 0) {
    StorageError err = MultipartUploadNext();
    if (err) {
      return err;
    }
  }

  statistics_.num_put_requests += 1;
  return uploader_.Finalize();
}

StorageError S3ObjectWriter::SinglePutUpload() {
  Aws::S3::Model::PutObjectRequest object_request;

  auto stream = std::make_shared<Aws::IOStream>(&buffer_);

  object_request.SetBucket(bucket_);
  object_request.SetKey(object_id_);
  object_request.SetBody(stream);
  auto put_object_outcome = client_->PutObject(object_request);
  if (!put_object_outcome.IsSuccess()) {
    return GetErrorFromOutcome(put_object_outcome);
  }

  ++statistics_.num_put_requests;
  statistics_.bytes_transferred += bytes_written_;

  return StorageError::Success();
}

StorageError S3ObjectWriter::MultipartUploadNext() {
  if (!uploader_.IsInitialized()) {
    StorageError upload_error = uploader_.Initialize();
    if (upload_error) {
      return upload_error;
    }
    ++statistics_.num_put_requests;
  }
  StorageError upload_error = uploader_.WriteChunk(&buffer_);
  if (upload_error) {
    return upload_error;
  }

  ++statistics_.num_put_requests;
  statistics_.bytes_transferred += bytes_written_;

  buffer_.str("");
  bytes_written_ = 0;
  return StorageError::Success();
}

S3Storage::S3Storage(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket)
    : client_(std::move(client)), bucket_(std::move(bucket)) {}

StorageError S3Storage::CreateBucket(const std::shared_ptr<const Aws::S3::S3Client>& client,
                                     const std::string& bucket) {
  Aws::S3::Model::CreateBucketRequest request;
  request.SetBucket(bucket);

  auto outcome = client->CreateBucket(request);
  if (!outcome.IsSuccess()) {
    return GetErrorFromOutcome(outcome);
  }
  return StorageError::Success();
}

StorageError S3Storage::DeleteBucket(const std::shared_ptr<const Aws::S3::S3Client>& client,
                                     const std::string& bucket) {
  Aws::S3::Model::DeleteBucketRequest request;
  request.SetBucket(bucket);

  auto outcome = client->DeleteBucket(request);
  if (!outcome.IsSuccess()) {
    return GetErrorFromOutcome(outcome);
  }
  return StorageError::Success();
}

StorageError S3Storage::Delete(const std::string& object_identifier) {
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(object_identifier);

  auto outcome = client_->DeleteObject(request);
  if (!outcome.IsSuccess()) {
    return GetErrorFromOutcome(outcome);
  }

  return StorageError::Success();
}

std::pair<std::vector<ObjectStatus>, StorageError> S3Storage::List(const std::string& object_prefix) {
  bool has_more = true;
  std::string continuation_token;
  StorageError error = StorageError::Success();
  std::vector<ObjectStatus> result_vector;

  while (has_more) {
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(bucket_);

    if (!object_prefix.empty()) {
      request.SetPrefix(object_prefix);
    }

    if (!continuation_token.empty()) {
      request.SetContinuationToken(continuation_token);
    }

    auto outcome = client_->ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
      error = GetErrorFromOutcome(outcome);
      break;
    }
    auto result = outcome.GetResult();

    if (result.GetIsTruncated()) {
      continuation_token = result.GetNextContinuationToken();
    } else {
      has_more = false;
    }

    for (const auto& obj : result.GetContents()) {
      result_vector.emplace_back(obj.GetKey(), ConvertAwsDateTime(obj.GetLastModified()), obj.GetETag(), obj.GetSize());
    }
  }

  return std::make_pair(result_vector, error);
}

S3MultipartUploader::S3MultipartUploader(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket,
                                         std::string object_id)
    : client_(std::move(client)),
      bucket_(std::move(bucket)),
      object_id_(std::move(object_id)),
      is_initialized_(false),
      part_count_(1) {}

StorageError S3MultipartUploader::Initialize() {
  Aws::S3::Model::CreateMultipartUploadRequest request;
  request.SetBucket(bucket_);
  request.SetKey(object_id_);
  auto outcome = client_->CreateMultipartUpload(request);
  if (!outcome.IsSuccess()) {
    return GetErrorFromOutcome(outcome);
  }
  upload_id_ = outcome.GetResult().GetUploadId();
  is_initialized_ = true;
  return StorageError::Success();
}

StorageError S3MultipartUploader::WriteChunk(std::stringbuf* buffer) {
  if (!IsInitialized()) {
    return StorageError(StorageErrorType::kInvalidState);
  }
  auto stream = std::make_shared<Aws::IOStream>(buffer);

  Aws::S3::Model::UploadPartRequest request;
  request.SetBucket(bucket_);
  request.SetKey(object_id_);
  request.SetUploadId(upload_id_);
  request.SetPartNumber(part_count_);
  request.SetBody(stream);

  auto outcome = client_->UploadPart(request);
  if (!outcome.IsSuccess()) {
    CancelUpload();
    return GetErrorFromOutcome(outcome);
  }

  parts_.push_back(Aws::S3::Model::CompletedPart().WithPartNumber(part_count_).WithETag(outcome.GetResult().GetETag()));
  ++part_count_;
  return StorageError::Success();
}

StorageError S3MultipartUploader::Finalize() {
  Aws::S3::Model::CompletedMultipartUpload details;
  details.SetParts(parts_);

  Aws::S3::Model::CompleteMultipartUploadRequest request;
  request.SetBucket(bucket_);
  request.SetKey(object_id_);
  request.SetUploadId(upload_id_);
  request.SetMultipartUpload(details);

  auto outcome = client_->CompleteMultipartUpload(request);
  if (!outcome.IsSuccess()) {
    CancelUpload();
    return GetErrorFromOutcome(outcome);
  }

  is_initialized_ = false;
  parts_.clear();
  part_count_ = 1;
  upload_id_ = "";

  return StorageError::Success();
}

S3MultipartUploader::~S3MultipartUploader() {
  if (IsInitialized()) {
    Finalize();
  }
}

StorageError S3MultipartUploader::CancelUpload() {
  if (!IsInitialized()) {
    return StorageError::Success();
  }
  Aws::S3::Model::AbortMultipartUploadRequest request;
  request.SetUploadId(upload_id_);
  request.SetBucket(bucket_);
  request.SetKey(object_id_);

  auto outcome = client_->AbortMultipartUpload(request);
  if (!outcome.IsSuccess()) {
    return GetErrorFromOutcome(outcome);
  }

  is_initialized_ = false;
  parts_.clear();
  part_count_ = 1;
  upload_id_ = "";
  return StorageError::Success();
}

S3ObjectReader::S3ObjectReader(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket,
                               std::string object_id)
    : client_(std::move(client)), bucket_(std::move(bucket)), object_id_(std::move(object_id)) {}

Aws::S3::Model::GetObjectRequest S3ObjectReader::CreateGetObjectRequest(std::vector<char>* buffer,
                                                                        const std::string& range) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(object_id_);
  request.SetResponseStreamFactory([this, buffer]() {
    buffer->clear();
    stream_.Reset(buffer);
    return new std::iostream(&stream_);
  });
  if (!range.empty()) {
    request.SetRange(range);
  }

  return request;
}

StorageError S3ObjectReader::Read(size_t first_byte, size_t last_byte, std::vector<char>* buffer) {
  bool read_entire_object = (first_byte == 0 && last_byte == kLastByteInFile);
  std::string range_string;
  if (!read_entire_object) {
    range_string = GetRangeString(first_byte, last_byte);
  }

  return ProcessGetObjectRequest(CreateGetObjectRequest(buffer, range_string));
}

StorageError S3ObjectReader::ReadTail(size_t num_last_bytes, std::vector<char>* buffer) {
  const std::string range_string = GetRangeStringForTail(num_last_bytes);
  return ProcessGetObjectRequest(CreateGetObjectRequest(buffer, range_string));
}

StorageError S3ObjectReader::ProcessGetObjectRequest(const Aws::S3::Model::GetObjectRequest& request) {
  auto outcome = client_->GetObject(request);
  if (!outcome.IsSuccess()) {
    return GetErrorFromOutcome(outcome);
  }

  // If we do not have status information about the object, we can obtain it now.
  if (status_.GetError().IsError()) {
    Aws::S3::Model::GetObjectResult& result = outcome.GetResult();
    const time_t last_modified = ConvertAwsDateTime(result.GetLastModified());
    const std::string& hash = result.GetETag();

    // For range requests, the actual length of the object is sent in the "Content-Range"-Header.
    const size_t size = result.GetContentRange().empty() ? result.GetContentLength()
                                                         : ParseContentLengthFromRange(result.GetContentRange());

    status_ = ObjectStatus(object_id_, last_modified, hash, size);
  }

  return StorageError::Success();
}

size_t S3ObjectReader::ParseContentLengthFromRange(const Aws::String& content_range) {
  // A header line might look like "Content-Range: bytes 0-1023/146515"
  // We are interested in the number after '/'.

  size_t index_of_slash = content_range.find_last_of('/');
  if (index_of_slash == Aws::String::npos) {
    Fail("Found a malformed value for header entry 'Content-Range'.");
  }

  Aws::String content_length_string = content_range.substr(index_of_slash + 1);
  try {
    return std::stoull(content_length_string);
  } catch (const std::exception& e) {
    // We have std::invalid_argument or std::out_of_range here.
    // A string of length 0 will also run into this branch.
    Fail(e.what());
  }
}

const ObjectStatus& S3ObjectReader::GetStatus() {
  if (status_.GetError()) {
    Aws::S3::Model::HeadObjectRequest request;

    request.SetBucket(bucket_);
    request.SetKey(object_id_);
    auto outcome = client_->HeadObject(request);

    if (!outcome.IsSuccess()) {
      status_ = ObjectStatus(GetErrorFromOutcome(outcome));
    } else {
      auto result = outcome.GetResult();

      const time_t last_modified = ConvertAwsDateTime(result.GetLastModified());
      const std::string& hash = result.GetETag();
      const size_t size = result.GetContentLength();

      status_ = ObjectStatus(object_id_, last_modified, hash, size);
    }
  }
  return status_;
}

std::string S3ObjectReader::GetRangeString(size_t first_byte, size_t last_byte) {
  std::stringstream stream;
  stream << "bytes=" << first_byte << "-";
  if (last_byte != kLastByteInFile) {
    stream << last_byte;
  }
  return stream.str();
}

std::string S3ObjectReader::GetRangeStringForTail(size_t num_last_bytes) {
  std::stringstream stream;
  stream << "bytes=-" << num_last_bytes;
  return stream.str();
}

StorageError S3ObjectReader::Close() { return StorageError::Success(); }

}  // namespace skyrise
