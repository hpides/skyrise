#include "s3_utils.hpp"

#include <algorithm>
#include <iterator>

#include <aws/core/utils/logging/LogMacros.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "constants.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

void CreateS3BucketIfNotExists(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& bucket_name) {
  const auto list_buckets_outcome = client->ListBuckets();

  if (!list_buckets_outcome.IsSuccess()) {
    Fail(list_buckets_outcome.GetError().GetMessage());
  }

  const auto& buckets = list_buckets_outcome.GetResult().GetBuckets();

  const auto contains_bucket_iterator = std::find_if(
      buckets.cbegin(), buckets.cend(), [&](const auto& bucket) { return bucket.GetName() == bucket_name; });

  if (contains_bucket_iterator == buckets.cend()) {
    const auto create_bucket_outcome =
        client->CreateBucket(Aws::S3::Model::CreateBucketRequest().WithBucket(bucket_name));

    if (!create_bucket_outcome.IsSuccess()) {
      Fail(create_bucket_outcome.GetError().GetMessage());
    }
  }
}

void UploadObjectToS3(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& object_key,
                      const std::shared_ptr<Aws::IOStream>& object_value, const size_t object_byte_size,
                      const Aws::String& bucket_name) {
  UploadObjectsToS3Parallel(client, {{object_key, object_value, object_byte_size}}, bucket_name);
}

void UploadObjectsToS3Parallel(
    const std::shared_ptr<const Aws::S3::S3Client>& client,
    const std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>>& objects,
    const Aws::String& bucket_name) {
  AWS_LOGSTREAM_INFO(kBaseTag.c_str(), "Uploading objects to S3...");

  std::vector<Aws::S3::Model::PutObjectOutcomeCallable> callables;
  callables.reserve(objects.size());

  // TODO(tobodner): Introduce a client-side thread pool
  for (const auto& [object_key, object, num_bytes] : objects) {
    auto put_object_request = Aws::S3::Model::PutObjectRequest().WithBucket(bucket_name).WithKey(object_key);
    put_object_request.SetBody(object);
    callables.emplace_back(client->PutObjectCallable(put_object_request));
  }

  size_t num_errors = 0;

  for (size_t i = 0; i < callables.size(); ++i) {
    const auto& outcome = callables[i].get();

    if (!outcome.IsSuccess()) {
      AWS_LOGSTREAM_ERROR(kBaseTag.c_str(), outcome.GetError().GetExceptionName()
                                                << ": " << outcome.GetError().GetMessage());
      Fail(outcome.GetError().GetMessage());
      num_errors++;
    } else {
      AWS_LOGSTREAM_INFO(kBaseTag.c_str(), std::get<0>(objects[i]) << " was uploaded successfully to S3.");
    }
  }

  if (num_errors > 0) {
    Fail(std::to_string(num_errors) + " errors during multi-threaded upload to S3.");
  }
}

bool ObjectExists(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& object_key,
                  const Aws::String& bucket_name) {
  Aws::S3::Model::HeadObjectRequest request;
  request.WithBucket(bucket_name).WithKey(object_key);
  const auto response = client->HeadObject(request);
  return response.IsSuccess();
}

void EmptyS3Bucket(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& bucket_name) {
  while (true) {
    const auto list_objects_outcome = client->ListObjects(Aws::S3::Model::ListObjectsRequest().WithBucket(bucket_name));
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
        client->DeleteObjects(Aws::S3::Model::DeleteObjectsRequest()
                                  .WithBucket(bucket_name)
                                  .WithDelete(Aws::S3::Model::Delete().WithObjects(objects_to_delete)));

    Assert(delete_objects_outcome.IsSuccess(), delete_objects_outcome.GetError().GetMessage());

    if (!list_objects_result.GetIsTruncated()) {
      break;
    }
  }
}

void EmptyAndDeleteS3Bucket(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& bucket_name) {
  EmptyS3Bucket(client, bucket_name);
  client->DeleteBucket(Aws::S3::Model::DeleteBucketRequest().WithBucket(bucket_name));
}

}  // namespace skyrise
