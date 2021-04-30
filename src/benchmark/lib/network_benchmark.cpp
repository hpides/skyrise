#include "network_benchmark.hpp"

#include <algorithm>
#include <numeric>
#include <unordered_map>

#include <magic_enum.hpp>

#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

NetworkBenchmark::NetworkBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                   std::shared_ptr<CostCalculator> cost_calculator,
                                   const std::vector<size_t>& object_byte_sizes,
                                   const std::vector<size_t>& thread_counts,
                                   const std::vector<size_t>& invocation_counts, const size_t batch_size,
                                   const size_t repetition_count)
    : Benchmark(std::move(cost_calculator)),
      helper_(std::move(helper)),
      object_byte_sizes_(object_byte_sizes),
      thread_counts_(thread_counts),
      invocation_counts_(invocation_counts),
      batch_size_(batch_size),
      repetition_count_(repetition_count),
      cost_overhead_(0) {
  Assert(!object_byte_sizes_.empty(), "Object byte sizes must not be empty.");
  Assert(!thread_counts_.empty(), "Thread counts must not be empty.");
  Assert(!invocation_counts_.empty(), "Invocation counts must not be empty.");
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> NetworkBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  Setup();

  std::vector<std::shared_ptr<BenchmarkResult>> results;
  results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    results.emplace_back(benchmark_runner->RunConfig(benchmark_config.second));
  }

  Teardown();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results_array(results.size());

  for (size_t i = 0; i < results.size(); i++) {
    results_array[i] = GenerateResultOutput(results[i], benchmark_configs_[i].first);
  }

  return results_array;
}

void NetworkBenchmark::Setup() {
  cost_overhead_ = 0;

  cost_overhead_ += helper_->CreateS3BucketIfNotExists(kReadBucket);
  cost_overhead_ += helper_->CreateS3BucketIfNotExists(kWriteBucket);

  cost_overhead_ += helper_->EmptyS3Bucket(kReadBucket);
  cost_overhead_ += helper_->EmptyS3Bucket(kWriteBucket);

  std::unordered_map<size_t, std::shared_ptr<Aws::IOStream>> s3_objects;
  s3_objects.reserve(object_byte_sizes_.size());

  for (const size_t object_byte_size : object_byte_sizes_) {
    s3_objects.emplace(object_byte_size, BenchmarkHelper::GenerateRandomObject(object_byte_size));
  }

  const size_t concurrency_maximum = *std::max_element(invocation_counts_.cbegin(), invocation_counts_.cend());
  const size_t thread_count_maximum = *std::max_element(thread_counts_.cbegin(), thread_counts_.cend());

  std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>> objects_to_upload;

  size_t bytes_uploading = 0;

  for (const size_t object_byte_size : object_byte_sizes_) {
    for (size_t i = 0; i < concurrency_maximum; i++) {
      for (size_t j = 0; j < thread_count_maximum; j++) {
        objects_to_upload.emplace_back(GenerateObjectKey(object_byte_size, i, j),
                                       BenchmarkHelper::GenerateRandomObject(object_byte_size), object_byte_size);
        bytes_uploading += object_byte_size;

        if (bytes_uploading >= kMaxMemoryUsageBytes) {
          cost_overhead_ += helper_->UploadObjectsToS3Parallel(objects_to_upload, kReadBucket);
          objects_to_upload.clear();
        }
      }
    }
  }

  cost_overhead_ += helper_->UploadObjectsToS3Parallel(objects_to_upload, kReadBucket);
}

void NetworkBenchmark::Teardown() {
  cost_overhead_ += helper_->EmptyS3Bucket(kReadBucket);
  cost_overhead_ += helper_->EmptyS3Bucket(kWriteBucket);
}

Aws::String NetworkBenchmark::GenerateObjectKey(const size_t object_byte_size, const size_t invocation_index,
                                                const size_t thread_index) {
  Aws::StringStream object_key;
  object_key << thread_index << "-" << invocation_index / kMaxObjectsPerPrefix << "/" << object_byte_size << "B-"
             << invocation_index;
  return object_key.str();
}

std::vector<std::shared_ptr<Aws::IOStream>> NetworkBenchmark::GeneratePayloads(const size_t function_instance_mb_size,
                                                                               const size_t object_byte_size,
                                                                               const size_t thread_count,
                                                                               const size_t invocation_count,
                                                                               const S3OperationType operation_type) {
  std::vector<std::shared_ptr<Aws::IOStream>> payloads;
  payloads.reserve(invocation_count);

  const bool is_parallel = std::any_of(invocation_counts_.cbegin(), invocation_counts_.cend(),
                                       [](const size_t invocation_count) { return invocation_count > 1; });

  for (size_t i = 0; i < invocation_count; i++) {
    Aws::Utils::Array<Aws::String> object_keys(thread_count);

    for (size_t j = 0; j < thread_count; j++) {
      Aws::StringStream object_key;
      object_key << GenerateObjectKey(object_byte_size, is_parallel ? i : 0, j);

      if (operation_type == S3OperationType::kWrite) {
        object_key << "-" << function_instance_mb_size << "MB";
      }
      object_keys[j] = object_key.str();
    }

    const auto payload = [&]() {
      auto payload_value =
          Aws::Utils::Json::JsonValue().WithArray("s3_keys", object_keys).WithInteger("batch_size", batch_size_);

      if (operation_type == S3OperationType::kRead) {
        return payload_value.WithString("s3_bucket", kReadBucket);
      } else {
        return payload_value.WithString("s3_bucket", kWriteBucket).WithInteger("object_byte_size", object_byte_size);
      }
    }();

    payloads.emplace_back(std::make_shared<Aws::StringStream>(payload.View().WriteCompact()));
  }

  return payloads;
}

}  // namespace skyrise
