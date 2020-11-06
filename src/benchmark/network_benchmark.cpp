#include "network_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

NetworkBenchmark::NetworkBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                   std::shared_ptr<CostCalculator> cost_calculator, const ExecuteMode execute_mode,
                                   size_t num_iterations, const Aws::String& read_bucket,
                                   const Aws::String& write_bucket,
                                   const std::vector<size_t>& function_instance_mb_sizes,
                                   const std::vector<size_t>& object_byte_sizes,
                                   const std::vector<size_t>& thread_counts)
    : helper_(std::move(helper)),
      cost_calculator_(std::move(cost_calculator)),
      execute_mode_(execute_mode),
      num_iterations_(num_iterations),
      read_bucket_(read_bucket),
      write_bucket_(write_bucket),
      cost_overhead_(0) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto object_byte_size : object_byte_sizes) {
      for (const auto thread_count : thread_counts) {
        if (thread_count * object_byte_size <= MbToByte(function_instance_mb_size) / 2) {
          BenchmarkConfig config(kFunctionName, function_instance_mb_size, num_iterations, execute_mode);
          config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_byte_size, thread_count));
          configs_.emplace_back(config,
                                NetworkBenchmarkParameters{function_instance_mb_size, object_byte_size, thread_count});
        }
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> NetworkBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  Setup();

  std::vector<std::tuple<std::shared_ptr<std::vector<BenchmarkItemResult>>, NetworkBenchmarkParameters>> results;

  for (const auto& [config, parameters] : configs_) {
    results.emplace_back(benchmark_runner->RunConfig(config), parameters);
  }

  Teardown();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results_array(results.size());

  for (size_t i = 0; i < results.size(); i++) {
    results_array[i] = GenerateResultOutput(std::get<0>(results[i]), std::get<1>(results[i]));
  }

  return results_array;
}

void NetworkBenchmark::Setup() {
  cost_overhead_ = 0;

  cost_overhead_ += helper_->CreateS3BucketIfNotExists(read_bucket_);
  cost_overhead_ += helper_->CreateS3BucketIfNotExists(write_bucket_);

  cost_overhead_ += helper_->EmptyS3Bucket(read_bucket_);
  cost_overhead_ += helper_->EmptyS3Bucket(write_bucket_);

  const bool is_parallel = execute_mode_ != ExecuteMode::ColdSequential && execute_mode_ != ExecuteMode::WarmSequential;

  for (const auto& [config, parameters] : configs_) {
    for (size_t i = 0; i < parameters.thread_count_; i++) {
      if (is_parallel) {
        for (size_t j = 0; j < num_iterations_; j++) {
          cost_overhead_ += helper_->UploadObjectToS3Bucket(
              read_bucket_, GenerateObjectKey(true, parameters.object_byte_size_, i, j),
              BenchmarkHelper::GenerateRandomObject(parameters.object_byte_size_), parameters.object_byte_size_);
        }
      } else {
        cost_overhead_ += helper_->UploadObjectToS3Bucket(
            read_bucket_, GenerateObjectKey(false, parameters.object_byte_size_, i),
            BenchmarkHelper::GenerateRandomObject(parameters.object_byte_size_), parameters.object_byte_size_);
      }
    }
  }
}

void NetworkBenchmark::Teardown() {
  cost_overhead_ += helper_->EmptyS3Bucket(read_bucket_);
  cost_overhead_ += helper_->EmptyS3Bucket(write_bucket_);
}

Aws::String NetworkBenchmark::GenerateObjectKey(const bool is_parallel, const size_t objects_byte_size,
                                                const size_t thread_index, const size_t iteration_index) {
  Aws::StringStream object_key;
  object_key << thread_index << "-" << (is_parallel ? std::to_string(iteration_index) : "") << objects_byte_size << "B-"
             << kObjectKeySuffix;
  return object_key.str();
}

std::vector<std::shared_ptr<Aws::IOStream>> NetworkBenchmark::GeneratePayloads(const size_t function_instance_mb_size,
                                                                               const size_t object_byte_size,
                                                                               const size_t thread_count) {
  const bool is_parallel = execute_mode_ != ExecuteMode::ColdSequential && execute_mode_ != ExecuteMode::WarmSequential;

  std::vector<std::shared_ptr<Aws::IOStream>> payloads;
  payloads.reserve(num_iterations_ * thread_count);

  for (size_t i = 0; i < num_iterations_; i++) {
    Aws::Utils::Array<Aws::String> read_object_keys(thread_count);
    Aws::Utils::Array<Aws::String> write_object_keys(thread_count);

    for (size_t j = 0; j < thread_count; j++) {
      read_object_keys[j] = GenerateObjectKey(is_parallel, object_byte_size, j, i);
      write_object_keys[j] = read_object_keys[j] + "-" + std::to_string(function_instance_mb_size) + "MB";
    }

    const auto payload_value = Aws::Utils::Json::JsonValue()
                                   .WithString("s3_bucket_read", read_bucket_)
                                   .WithArray("s3_keys_read", read_object_keys)
                                   .WithString("s3_bucket_write", read_bucket_)
                                   .WithArray("s3_keys_write", write_object_keys);

    payloads.emplace_back(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));
  }
  return payloads;
}

long double NetworkBenchmark::ExtractFunctionCost(const BenchmarkItemResult& result,
                                                  const size_t function_instance_mb_size) {
  const double billed_duration = BenchmarkHelper::ExtractBilledLambdaDuration(result);
  const long double function_instance_cost =
      cost_calculator_->CalculateCostLambda(billed_duration, function_instance_mb_size);

  const auto payload_value = Aws::Utils::Json::JsonValue(StreamToString(&result.invoke_result->GetPayload()));
  const auto payload_view = payload_value.View();

  const size_t num_s3_requests_tier_1 = payload_view.GetInteger("num_s3_requests_tier_1");
  const size_t num_s3_requests_tier_2 = payload_view.GetInteger("num_s3_requests_tier_2");
  const size_t s3_storage_used_byte = payload_view.GetInt64("s3_storage_used_byte");

  const long double s3_request_cost =
      cost_calculator_->CalculateCostS3Requests(num_s3_requests_tier_1, num_s3_requests_tier_2);

  // TODO(d-justen): Find a way to track actual hours. For now, we assume that S3 object in this benchmark will be
  // deleted within an hour.
  const long double s3_storage_cost = cost_calculator_->CalculateCostS3StorageMonthly(s3_storage_used_byte, 1);

  return function_instance_cost + s3_request_cost + s3_storage_cost;
}

long double NetworkBenchmark::CalculateBenchmarkCost(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                                     const size_t function_instance_mb_size) {
  std::vector<long double> function_costs;
  function_costs.reserve(result->size());

  std::transform(
      result->cbegin(), result->cend(), std::back_inserter(function_costs),
      [&](const BenchmarkItemResult& result) { return ExtractFunctionCost(result, function_instance_mb_size); });
  const long double benchmark_cost = std::accumulate(function_costs.cbegin(), function_costs.cend(), 0.0l);

  return benchmark_cost;
}

}  // namespace skyrise
