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
                                   const std::vector<size_t>& bucket_counts)
    : Benchmark(std::move(cost_calculator)), helper_(std::move(helper)), bucket_counts_(bucket_counts) {
  Assert(!bucket_counts_.empty(), "Bucket counts must not be empty.");
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
  const size_t bucket_count = *std::max_element(bucket_counts_.cbegin(), bucket_counts_.cend());

  for (size_t i = 0; i < bucket_count; i++) {
    helper_->CreateS3BucketIfNotExists(kBucketPrefix + std::to_string(i));
    helper_->EmptyS3Bucket(kBucketPrefix + std::to_string(i));
  }
}

void NetworkBenchmark::Teardown() {
  const size_t bucket_count = *std::max_element(bucket_counts_.cbegin(), bucket_counts_.cend());

  for (size_t i = 0; i < bucket_count; i++) {
    helper_->EmptyAndDeleteS3Bucket(kBucketPrefix + std::to_string(i));
  }
}

Aws::String NetworkBenchmark::GenerateObjectKey(const size_t object_byte_size, const size_t invocation_index,
                                                const size_t thread_index) {
  Aws::StringStream object_key;
  object_key << thread_index << "-" << invocation_index / kMaxObjectsPerPrefix << "/" << object_byte_size << "B-"
             << invocation_index;
  return object_key.str();
}

std::vector<std::shared_ptr<Aws::IOStream>> NetworkBenchmark::GeneratePayloads(
    const NetworkBenchmarkParameters& parameters) {
  std::vector<std::shared_ptr<Aws::IOStream>> payloads;
  payloads.reserve(parameters.invocation_count);

  for (size_t i = 0; i < parameters.invocation_count; i++) {
    Aws::Utils::Array<Aws::String> object_keys(parameters.thread_count);

    for (size_t j = 0; j < parameters.thread_count; j++) {
      Aws::StringStream object_key;
      object_key << GenerateObjectKey(parameters.object_byte_size, parameters.invocation_count > 1 ? i : 0, j);

      object_keys[j] = object_key.str();
    }

    const auto payload_value = Aws::Utils::Json::JsonValue()
                                   .WithArray("s3_keys", object_keys)
                                   .WithInteger("batch_size", parameters.batch_size)
                                   .WithString("s3_bucket", kBucketPrefix + std::to_string(parameters.invocation_count %
                                                                                           parameters.bucket_count))
                                   .WithInteger("object_byte_size", parameters.object_byte_size);

    payloads.emplace_back(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));
  }

  return payloads;
}

}  // namespace skyrise
