#pragma once

#include <tuple>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

#include "storage/backend/s3_storage.hpp"
#include "storage/formats/abstract_chunk_reader.hpp"

namespace skyrise {

void CreateS3BucketIfNotExists(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& bucket_name);

void UploadObjectToS3(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& object_key,
                      const std::shared_ptr<Aws::IOStream>& object_value, const size_t object_byte_size,
                      const Aws::String& bucket_name);

void UploadObjectsToS3Parallel(
    const std::shared_ptr<const Aws::S3::S3Client>& client,
    const std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>>& objects,
    const Aws::String& bucket_name);

bool ObjectExists(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& object_key,
                  const Aws::String& bucket_name);

void EmptyS3Bucket(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& bucket_name);

// All objects in an S3 bucket must be deleted before the bucket can be deleted.
void EmptyAndDeleteS3Bucket(const std::shared_ptr<const Aws::S3::S3Client>& client, const Aws::String& bucket_name);

}  // namespace skyrise
