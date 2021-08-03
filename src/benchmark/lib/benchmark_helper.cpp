#include "benchmark_helper.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iterator>
#include <regex>

#include <aws/core/utils/logging/LogMacros.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

// TODO(anyone): Add Git commit hash to logging tag
inline const std::string kTag{"SKYRISE/BENCHMARK/BENCHMARK_HELPER"};

void BenchmarkHelper::CreateS3BucketIfNotExists(const Aws::String& bucket_name) const {
  const auto list_buckets_outcome = s3_client_->ListBuckets();

  if (!list_buckets_outcome.IsSuccess()) {
    Fail(list_buckets_outcome.GetError().GetMessage());
  }

  const auto& buckets = list_buckets_outcome.GetResult().GetBuckets();

  const auto contains_bucket_iterator = std::find_if(
      buckets.cbegin(), buckets.cend(), [&](const auto& bucket) { return bucket.GetName() == bucket_name; });

  if (contains_bucket_iterator == buckets.cend()) {
    const auto create_bucket_outcome =
        s3_client_->CreateBucket(Aws::S3::Model::CreateBucketRequest().WithBucket(bucket_name));

    if (!create_bucket_outcome.IsSuccess()) {
      Fail(create_bucket_outcome.GetError().GetMessage());
    }
  }
}

std::shared_ptr<Aws::IOStream> BenchmarkHelper::GenerateRandomObject(const size_t num_bytes) {
  return std::make_shared<Aws::StringStream>(RandomString(num_bytes));
}

void BenchmarkHelper::UploadObjectToS3(const Aws::String& object_key,
                                       const std::shared_ptr<Aws::IOStream>& object_value,
                                       const size_t object_byte_size, const Aws::String& bucket_name) const {
  UploadObjectsToS3Parallel({{object_key, object_value, object_byte_size}}, bucket_name);
}

void BenchmarkHelper::UploadObjectsToS3Parallel(
    const std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>>& objects,
    const Aws::String& bucket_name) const {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Uploading objects to S3...");

  std::vector<Aws::S3::Model::PutObjectOutcomeCallable> callables;
  callables.reserve(objects.size());

  // TODO(anyone): Introduce a client-side thread pool
  for (const auto& [object_key, object, num_bytes] : objects) {
    auto put_object_request = Aws::S3::Model::PutObjectRequest().WithBucket(bucket_name).WithKey(object_key);
    put_object_request.SetBody(object);
    callables.emplace_back(s3_client_->PutObjectCallable(put_object_request));
  }

  size_t num_errors = 0;

  for (size_t i = 0; i < callables.size(); i++) {
    const auto& outcome = callables[i].get();

    if (!outcome.IsSuccess()) {
      AWS_LOGSTREAM_ERROR(kTag.c_str(), outcome.GetError().GetExceptionName()
                                            << ": " << outcome.GetError().GetMessage());
      num_errors++;
    } else {
      AWS_LOGSTREAM_INFO(kTag.c_str(), std::get<0>(objects[i]) << " was uploaded successfully to S3.");
    }
  }

  if (num_errors > 0) {
    Fail(std::to_string(num_errors) + " errors during multi-threaded upload to S3.");
  }
}

void BenchmarkHelper::EmptyS3Bucket(const Aws::String& bucket_name) const {
  while (true) {
    const auto list_objects_outcome =
        s3_client_->ListObjects(Aws::S3::Model::ListObjectsRequest().WithBucket(bucket_name));
    Assert(list_objects_outcome.IsSuccess(), list_objects_outcome.GetError().GetMessage());

    const auto& list_objects_result = list_objects_outcome.GetResult();
    const auto& listed_objects = list_objects_result.GetContents();

    if (listed_objects.empty()) {
      break;
    }

    Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects_to_delete;
    std::transform(listed_objects.cbegin(), listed_objects.cend(), std::back_inserter(objects_to_delete),
                   [](const auto& object) { return Aws::S3::Model::ObjectIdentifier().WithKey(object.GetKey()); });

    const auto delete_objects_outcome =
        s3_client_->DeleteObjects(Aws::S3::Model::DeleteObjectsRequest()
                                      .WithBucket(bucket_name)
                                      .WithDelete(Aws::S3::Model::Delete().WithObjects(objects_to_delete)));

    Assert(delete_objects_outcome.IsSuccess(), delete_objects_outcome.GetError().GetMessage());

    if (!list_objects_result.GetIsTruncated()) {
      break;
    }
  }
}

void BenchmarkHelper::EmptyAndDeleteS3Bucket(const Aws::String& bucket_name) const {
  EmptyS3Bucket(bucket_name);
  s3_client_->DeleteBucket(Aws::S3::Model::DeleteBucketRequest().WithBucket(bucket_name));
}

}  // namespace skyrise
