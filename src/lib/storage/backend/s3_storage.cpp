#include "s3_storage.hpp"

#include "configuration.hpp"
#include "constants.hpp"
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
      multipart_uploader_(client_, bucket, object_id) {
  parts_.emplace_back(std::make_shared<std::stringbuf>());
}

S3ObjectWriter::~S3ObjectWriter() { FinalizeUpload(); }

StorageError S3ObjectWriter::Write(const char* data, size_t length) {
  if (closed_) {
    return StorageError(StorageErrorType::kInvalidState);
  }

  if (length == 0) {
    return StorageError::Success();
  }

  auto current_part = parts_.back();

  const size_t expected_buffer_size = current_part_size_bytes_ + length;

  if (!use_multipart_upload_ && expected_buffer_size > kMultipartThresholdBytes) {
    use_multipart_upload_ = true;
    statistics_.was_multipart_upload = true;
    multipart_uploader_.Initialize();
    ++statistics_.put_request_count;
  }

  if (use_multipart_upload_ && expected_buffer_size > kMultipartSizeBytes) {
    while (length + current_part_size_bytes_ >= kMultipartSizeBytes) {
      const size_t left_to_write = kMultipartSizeBytes - current_part_size_bytes_;
      if (left_to_write > 0) {
        current_part->sputn(data, left_to_write);
        current_part_size_bytes_ += left_to_write;
        length -= left_to_write;
        data += left_to_write;
      }

      UploadNextPartAsync();
      current_part = parts_.back();
    }
  }

  current_part->sputn(data, length);
  current_part_size_bytes_ += length;

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

  if (current_part_size_bytes_ > 0) {
    UploadNextPartAsync();
  }

  statistics_.put_request_count += 1;
  return multipart_uploader_.Finalize();
}

StorageError S3ObjectWriter::SinglePutUpload() {
  Aws::S3::Model::PutObjectRequest request;

  if (parts_.size() > 1) {
    return StorageError(StorageErrorType::kInternalError);
  }

  auto stream = std::make_shared<Aws::IOStream>(parts_.back().get());
  request.SetBucket(bucket_);
  request.SetKey(object_id_);
  request.SetBody(stream);
  auto put_object_outcome = client_->PutObject(request);
  if (!put_object_outcome.IsSuccess()) {
    return GetErrorFromOutcome(put_object_outcome);
  }

  ++statistics_.put_request_count;
  statistics_.bytes_transferred += current_part_size_bytes_;

  return StorageError::Success();
}

void S3ObjectWriter::UploadNextPartAsync() {
  multipart_uploader_.UploadPartAsync(parts_.back());

  ++statistics_.put_request_count;
  statistics_.bytes_transferred += current_part_size_bytes_;

  current_part_size_bytes_ = 0;
  parts_.emplace_back(std::make_shared<std::stringbuf>());
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
    : client_(std::move(client)), bucket_(std::move(bucket)), object_id_(std::move(object_id)) {}

void S3MultipartUploader::Initialize() {
  Aws::S3::Model::CreateMultipartUploadRequest request;
  request.SetBucket(bucket_);
  request.SetKey(object_id_);
  auto outcome = client_->CreateMultipartUpload(request);
  if (!outcome.IsSuccess()) {
    SetError(GetErrorFromOutcome(outcome));
  }
  upload_id_ = outcome.GetResult().GetUploadId();
  is_initialized_ = true;
}

StorageError S3MultipartUploader::Finalize() {
  std::unique_lock<std::mutex> lock(multipart_outcome_mutex_);
  multipart_upload_finished_condition_.wait(lock, [this] { return in_process_parts_count_ == 0; });

  if (storage_error_.IsError()) {
    return storage_error_;
  }

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

void S3MultipartUploader::UploadPartAsync(const std::shared_ptr<std::stringbuf>& buffer) {
  auto stream = std::make_shared<Aws::IOStream>(buffer.get());

  const int part_number = part_count_;
  Aws::S3::Model::UploadPartRequest request;
  request.SetBucket(bucket_);
  request.SetKey(object_id_);
  request.SetUploadId(upload_id_);
  request.SetPartNumber(part_number);
  request.SetBody(stream);

  const auto callback = [this, part_number, buffer](
                            const Aws::S3::S3Client* /*client*/, const Aws::S3::Model::UploadPartRequest& /*request*/,
                            const Aws::S3::Model::UploadPartOutcome& outcome,
                            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& /*context*/) {
    if (!outcome.IsSuccess()) {
      CancelUpload();
      return SetError(GetErrorFromOutcome(outcome));
    }

    std::lock_guard<std::mutex> lock_guard(multipart_outcome_mutex_);
    const int part_index = part_number - 1;
    parts_[part_index] =
        Aws::S3::Model::CompletedPart().WithPartNumber(part_number).WithETag(outcome.GetResult().GetETag());
    in_process_parts_count_--;
    buffer->str("");
    multipart_upload_finished_condition_.notify_one();
  };

  in_process_parts_count_++;
  part_count_++;
  parts_.emplace_back();
  client_->UploadPartAsync(request, callback);
}

void S3MultipartUploader::SetError(const StorageError& error) {
  std::lock_guard<std::mutex> lock_guard(multipart_outcome_mutex_);
  storage_error_ = error;
}

S3ObjectReader::S3ObjectReader(std::shared_ptr<const Aws::S3::S3Client> client, std::string bucket,
                               std::string object_id)
    : client_(std::move(client)), bucket_(std::move(bucket)), object_id_(std::move(object_id)) {}

Aws::S3::Model::GetObjectRequest S3ObjectReader::CreateGetObjectRequest(ByteBuffer* buffer, const std::string& range) {
  const auto stream = std::make_shared<DelegateStreamBuffer>();
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(object_id_);
  request.SetResponseStreamFactory([buffer, stream]() {
    // TODO(tobodner): Investigate whether we can remove the ByteBuffer::Resize method.
    buffer->Resize(0);
    stream->Reset(buffer);
    return new std::iostream(stream.get());
  });

  if (!range.empty()) {
    request.SetRange(range);
  }
  return request;
}

StorageError S3ObjectReader::Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) {
  const bool read_entire_object = (first_byte == 0 && last_byte == kLastByteInFile);
  std::string range_string;
  if (!read_entire_object) {
    range_string = GetRangeString(first_byte, last_byte);
  }

  return ProcessGetObjectRequest(CreateGetObjectRequest(buffer, range_string));
}

StorageError S3ObjectReader::ReadTail(size_t num_last_bytes, ByteBuffer* buffer) {
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
    const Aws::S3::Model::GetObjectResult& result = outcome.GetResult();
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

  const size_t index_of_slash = content_range.find_last_of('/');
  if (index_of_slash == Aws::String::npos) {
    Fail("Found a malformed value for header entry 'Content-Range'.");
  }

  const Aws::String content_length_string = content_range.substr(index_of_slash + 1);
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

StorageError S3ObjectReader::ReadObjectAsync(const std::shared_ptr<ObjectBuffer>& buffer,
                                             std::optional<std::vector<std::pair<size_t, size_t>>> ranges_optional) {
  const std::vector<std::pair<size_t, size_t>> byte_ranges =
      BuildByteRanges(ranges_optional, GetObjectSize(), kS3ReadRequestSizeBytes, kParquetFooterSizeBytes);
  const auto get_requests = BuildRequestsFromByteRanges(byte_ranges, buffer);
  return ProcessGetObjectRequestsAsync(get_requests);
}

std::shared_ptr<std::vector<Aws::S3::Model::GetObjectRequest>> S3ObjectReader::BuildRequestsFromByteRanges(
    const std::vector<std::pair<size_t, size_t>>& ranges, const std::shared_ptr<ObjectBuffer>& buffer) {
  auto requests = std::make_shared<std::vector<Aws::S3::Model::GetObjectRequest>>();
  requests->reserve(ranges.size());

  for (const auto& [first_byte, last_byte] : ranges) {
    bool read_entire_object = (first_byte == 0 && last_byte == kLastByteInFile);
    std::string range_string;
    if (!read_entire_object) {
      range_string = GetRangeString(first_byte, last_byte);
    }
    const auto byte_buffer = std::make_shared<ByteBuffer>(last_byte - first_byte);
    requests->push_back(CreateGetObjectRequest(byte_buffer.get(), range_string));
    buffer->AddBuffer({{first_byte, last_byte}, byte_buffer});
  }
  return requests;
}

StorageError S3ObjectReader::ProcessGetObjectRequestsAsync(
    const std::shared_ptr<std::vector<Aws::S3::Model::GetObjectRequest>>& requests) {
  std::unique_lock<std::mutex> lock(mutex_);
  StorageError storage_error = StorageError::Success();
  size_t remaining_request_count = requests->size();

  const auto callback =
      [this, &storage_error, &remaining_request_count](
          const Aws::S3::S3Client* /*s3_client*/, const Aws::S3::Model::GetObjectRequest& /*request*/,
          const Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult /*result*/, Aws::S3::S3Error>& outcome,
          const std::shared_ptr<const Aws::Client::AsyncCallerContext>& /*context*/) {
        std::lock_guard<std::mutex> lock_guard(mutex_);
        remaining_request_count--;
        if (!outcome.IsSuccess()) {
          storage_error = GetErrorFromOutcome(outcome);
        }
        condition_variable_.notify_one();
      };

  for (const auto& request : *requests) {
    client_->GetObjectAsync(request, callback);
  }

  condition_variable_.wait(lock, [&remaining_request_count] { return remaining_request_count == 0; });
  return storage_error;
}

size_t S3ObjectReader::GetObjectSize() {
  if (!object_size_.has_value()) {
    const ObjectStatus& status = GetStatus();
    if (status.GetError()) {
      throw std::logic_error("Could not get status of object");  // Signal failure to std::istream.
    }
    *object_size_ = status.GetSize();
  }

  return *object_size_;
}

}  // namespace skyrise
