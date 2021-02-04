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
    : helper_(std::move(helper)),
      cost_calculator_(std::move(cost_calculator)),
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
                                                const size_t thread_index) const {
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

long double NetworkBenchmark::ExtractFunctionCost(const InvocationResult& result,
                                                  const size_t function_instance_mb_size) {
  const double billed_duration = BenchmarkHelper::ExtractBilledLambdaDuration(result);
  const long double function_instance_cost =
      cost_calculator_->CalculateCostLambda(billed_duration, function_instance_mb_size);

  const auto payload_value = Aws::Utils::Json::JsonValue(StreamToString(&result.invoke_result_->GetPayload()));
  const auto payload_view = payload_value.View();

  const size_t num_s3_requests_tier_1 = payload_view.GetInteger("num_s3_requests_tier_1");
  const size_t num_s3_requests_tier_2 = payload_view.GetInteger("num_s3_requests_tier_2");
  const size_t s3_storage_used_bytes = payload_view.GetInt64("s3_storage_used_bytes");

  const long double s3_request_cost =
      cost_calculator_->CalculateCostS3Requests(num_s3_requests_tier_1, num_s3_requests_tier_2);

  // TODO(d-justen): Find a way to track actual hours. For now, we assume that S3 object in this benchmark will be
  // deleted within an hour.
  const long double s3_storage_cost = cost_calculator_->CalculateCostS3StorageMonthly(s3_storage_used_bytes, 1);

  return function_instance_cost + s3_request_cost + s3_storage_cost;
}

long double NetworkBenchmark::CalculateBenchmarkCost(const std::shared_ptr<BenchmarkResult>& result,
                                                     const size_t function_instance_mb_size) {
  const auto& invocation_results = result->GetInvocationResults();

  std::vector<long double> function_costs;
  function_costs.reserve(invocation_results.size() * invocation_results.front().size());

  for (const auto& repetition : invocation_results) {
    std::transform(repetition.cbegin(), repetition.cend(), std::back_inserter(function_costs),
                   [&](const std::pair<Aws::String, InvocationResult>& map_entry) {
                     return ExtractFunctionCost(map_entry.second, function_instance_mb_size);
                   });
  }

  const long double benchmark_cost = std::accumulate(function_costs.cbegin(), function_costs.cend(), 0.0l);

  return benchmark_cost;
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> NetworkBenchmark::GenerateBatchedSubResultOutput(
    const std::map<Aws::String, InvocationResult>& sub_result, const Aws::String& benchmark_name,
    const size_t function_instance_mb_size, const Aws::String& metric_name,
    const std::function<double(const double)>& process_value) {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> batched_runs(sub_result.size());

  size_t i = 0;
  for (const auto& invocation_result : sub_result) {
    Aws::Utils::Json::JsonValue result_value(StreamToString(&invocation_result.second.invoke_result_->GetPayload()));
    const auto duration_views = result_value.View().GetArray("ms_durations");

    Aws::Utils::Array<Aws::Utils::Json::JsonValue> duration_values(duration_views.GetLength());

    for (size_t j = 0; j < duration_views.GetLength(); j++) {
      const double duration_ms = process_value(duration_views[j].AsDouble());
      duration_values[j] = Aws::Utils::Json::JsonValue().AsDouble(duration_ms);
    }

    batched_runs[i] =
        Aws::Utils::Json::JsonValue()
            .WithString("name", benchmark_name + "/" + std::to_string(i))
            .WithArray(metric_name, duration_values)
            .WithDouble("billed_lambda_duration_ms",
                        BenchmarkHelper::ExtractBilledLambdaDuration(invocation_result.second))
            .WithDouble("function_cost_usd",
                        static_cast<double>(ExtractFunctionCost(invocation_result.second, function_instance_mb_size)));
    i++;
  }

  return batched_runs;
}

std::vector<double> NetworkBenchmark::ExtractValuesFromBatchedSubResults(
    const Aws::Utils::Array<Aws::Utils::Json::JsonValue>& batched_runs, const Aws::String& metric_name) const {
  std::vector<double> values;
  values.reserve(batched_runs.GetLength() * batch_size_);

  for (size_t i = 0; i < batched_runs.GetLength(); i++) {
    const auto batch = batched_runs[i].View().GetArray(metric_name);

    for (size_t j = 0; j < batch.GetLength(); j++) {
      values.emplace_back(batch[j].AsDouble());
    }
  }

  return values;
}

}  // namespace skyrise
