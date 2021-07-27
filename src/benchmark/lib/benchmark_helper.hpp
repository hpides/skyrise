#pragma once

#include <functional>
#include <optional>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark_runner.hpp"
#include "client/client.hpp"

namespace skyrise {

// TODO(maltenbergert): Refactor BenchmarkHelper into an S3Helper
class BenchmarkHelper {
 public:
  BenchmarkHelper(std::shared_ptr<Client> client) : client_(client) {}

  void CreateS3BucketIfNotExists(const Aws::String& bucket_name) const;
  static std::shared_ptr<Aws::IOStream> GenerateRandomObject(const size_t num_bytes);
  void UploadObjectToS3(const Aws::String& object_key, const std::shared_ptr<Aws::IOStream>& object_value,
                        const size_t object_byte_size, const Aws::String& bucket_name) const;
  void UploadObjectsToS3Parallel(
      const std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>>& objects,
      const Aws::String& bucket_name) const;
  void EmptyS3Bucket(const Aws::String& bucket_name) const;
  // All objects in an S3 bucket must be deleted before the bucket can be deleted
  void EmptyAndDeleteS3Bucket(const Aws::String& bucket_name) const;

 private:
  const std::shared_ptr<Client> client_;
};

}  // namespace skyrise
