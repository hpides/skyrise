#include "network_throughput_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include <magic_enum.hpp>

#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

NetworkThroughputBenchmark::NetworkThroughputBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                       std::shared_ptr<CostCalculator> cost_calculator,
                                                       const std::vector<size_t>& function_instance_mb_sizes,
                                                       const std::vector<size_t>& object_mb_sizes,
                                                       const std::vector<size_t>& thread_counts,
                                                       ExecuteMode execute_mode, size_t num_iterations)
    : helper_(std::move(helper)),
      cost_calculator_(std::move(cost_calculator)),
      function_instance_mb_sizes_(function_instance_mb_sizes),
      object_mb_sizes_(object_mb_sizes),
      thread_counts_(thread_counts),
      execute_mode_(execute_mode),
      num_iterations_(num_iterations),
      cost_overhead_(0) {}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> NetworkThroughputBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  Setup();

  std::vector<NetworkThroughputBenchmarkResult> results;

  for (const auto function_instance_mb_size : function_instance_mb_sizes_) {
    for (const auto object_mb_size : object_mb_sizes_) {
      for (const auto thread_count : thread_counts_) {
        if (thread_count * object_mb_size <= function_instance_mb_size / 2) {
          BenchmarkConfig config(kFunctionName, function_instance_mb_size, num_iterations_, execute_mode_);
          config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_mb_size, thread_count));

          const auto result = benchmark_runner->RunConfig(config);
          results.emplace_back(
              NetworkThroughputBenchmarkResult{function_instance_mb_size, object_mb_size, thread_count, result});
        }
      }
    }
  }

  Teardown();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results_array(results.size());

  for (size_t i = 0; i < results.size(); i++) {
    results_array[i] = GenerateResultOutput(results[i], results.size());
  }

  return results_array;
}

void NetworkThroughputBenchmark::Setup() {
  cost_overhead_ += helper_->CreateS3BucketIfNotExists(kReadBucket);
  cost_overhead_ += helper_->CreateS3BucketIfNotExists(kWriteBucket);

  cost_overhead_ += helper_->EmptyS3Bucket(kReadBucket);
  cost_overhead_ += helper_->EmptyS3Bucket(kWriteBucket);

  const bool is_parallel = execute_mode_ != ExecuteMode::ColdSequential && execute_mode_ != ExecuteMode::WarmSequential;

  for (const auto& object_mb_size : object_mb_sizes_) {
    for (const auto& thread_count : thread_counts_) {
      for (size_t i = 0; i < thread_count; i++) {
        const size_t object_size_bytes = MbToByte(object_mb_size);

        if (is_parallel) {
          for (size_t j = 0; j < num_iterations_; j++) {
            const Aws::String object_key =
                kObjectKey + "-" + std::to_string(object_mb_size) + "MB-" + std::to_string(i) + "-" + std::to_string(j);
            cost_overhead_ += helper_->UploadObjectToS3Bucket(
                kReadBucket, object_key, BenchmarkHelper::GenerateRandomObject(object_size_bytes), object_size_bytes);
          }
        } else {
          const Aws::String object_key = kObjectKey + "-" + std::to_string(object_mb_size) + "MB-" + std::to_string(i);
          cost_overhead_ += helper_->UploadObjectToS3Bucket(
              kReadBucket, object_key, BenchmarkHelper::GenerateRandomObject(object_size_bytes), object_size_bytes);
        }
      }
    }
  }
}

void NetworkThroughputBenchmark::Teardown() {
  helper_->EmptyS3Bucket(kReadBucket);
  helper_->EmptyS3Bucket(kWriteBucket);
}

long double NetworkThroughputBenchmark::CalculateBenchmarkCost(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const size_t function_instance_mb_size) {
  std::vector<long double> function_costs;
  function_costs.reserve(result->size());

  std::transform(
      result->cbegin(), result->cend(), std::back_inserter(function_costs),
      [&](const BenchmarkItemResult& result) { return ExtractFunctionCost(result, function_instance_mb_size); });
  const long double benchmark_cost = std::accumulate(function_costs.cbegin(), function_costs.cend(), 0.0l);

  return benchmark_cost;
}

std::vector<std::shared_ptr<Aws::IOStream>> NetworkThroughputBenchmark::GeneratePayloads(
    const size_t function_instance_mb_size, const size_t object_mb_size, const size_t thread_count) {
  const bool is_parallel = execute_mode_ != ExecuteMode::ColdSequential && execute_mode_ != ExecuteMode::WarmSequential;

  std::vector<std::shared_ptr<Aws::IOStream>> payloads;
  payloads.reserve(num_iterations_ * thread_count);

  for (size_t i = 0; i < num_iterations_; i++) {
    Aws::Utils::Array<Aws::String> read_object_keys(thread_count);
    Aws::Utils::Array<Aws::String> write_object_keys(thread_count);

    for (size_t j = 0; j < thread_count; j++) {
      read_object_keys[j] = kObjectKey + "-" + std::to_string(object_mb_size) + "MB-" + std::to_string(j) +
                            (is_parallel ? "-" + std::to_string(i) : "");

      write_object_keys[j] = kObjectKey + "-" + std::to_string(function_instance_mb_size) + "MB-" +
                             std::to_string(object_mb_size) + "MB-" + std::to_string(j) + "-" + std::to_string(i);
    }

    const auto payload_value = Aws::Utils::Json::JsonValue()
                                   .WithString("s3_bucket_read", kReadBucket)
                                   .WithArray("s3_keys_read", read_object_keys)
                                   .WithString("s3_bucket_write", kWriteBucket)
                                   .WithArray("s3_keys_write", write_object_keys);

    payloads.emplace_back(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));
  }
  return payloads;
}

Aws::Utils::Json::JsonValue NetworkThroughputBenchmark::GenerateResultOutput(
    const NetworkThroughputBenchmarkResult& result, const size_t num_results) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkThroughputBenchmark/";
  benchmark_name << magic_enum::enum_name(execute_mode_) << "/" << result.function_instance_mb_size_ << "MB/"
                 << result.object_mb_size_ << "MB/" << result.thread_count_;

  const auto extract_duration_seconds = [&](const BenchmarkItemResult& single_result, const Aws::String& key) {
    return std::chrono::duration<double>(
               std::chrono::duration<double, std::milli>(helper_->ExtractMetric(single_result, key)))
        .count();
  };

  const auto get_throughput_aggregates =
      BenchmarkHelper::CalculateAggregates(result.results_, [&](const BenchmarkItemResult& single_result) {
        return static_cast<double>(result.object_mb_size_ * result.thread_count_) /
               extract_duration_seconds(single_result, kJsonGetObjectDurationKey);
      });

  const auto put_throughput_aggregates =
      BenchmarkHelper::CalculateAggregates(result.results_, [&](const BenchmarkItemResult& single_result) {
        return static_cast<double>(result.object_mb_size_ * result.thread_count_) /
               extract_duration_seconds(single_result, kJsonPutObjectDurationKey);
      });

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"get_object_throughput_mb_per_s_average", get_throughput_aggregates.average},
       {"get_object_throughput_mb_per_s_minimum", get_throughput_aggregates.minimum},
       {"get_object_throughput_mb_per_s_median", get_throughput_aggregates.median},
       {"get_object_throughput_mb_per_s_maximum", get_throughput_aggregates.maximum},
       {"get_object_throughput_mb_per_s_percentile_90", get_throughput_aggregates.percentile_90},
       {"get_object_throughput_mb_per_s_percentile_99", get_throughput_aggregates.percentile_99},
       {"get_object_throughput_mb_per_s_percentile_99.9", get_throughput_aggregates.percentile_99_9},
       {"get_object_throughput_mb_per_s_percentile_99.99", get_throughput_aggregates.percentile_99_99},
       {"get_object_throughput_mb_per_s_std_dev", get_throughput_aggregates.standard_deviation},
       {"put_object_throughput_mb_per_s_average", put_throughput_aggregates.average},
       {"put_object_throughput_mb_per_s_minimum", put_throughput_aggregates.minimum},
       {"put_object_throughput_mb_per_s_median", put_throughput_aggregates.median},
       {"put_object_throughput_mb_per_s_maximum", put_throughput_aggregates.maximum},
       {"put_object_throughput_mb_per_s_percentile_90", put_throughput_aggregates.percentile_90},
       {"put_object_throughput_mb_per_s_percentile_99", put_throughput_aggregates.percentile_99},
       {"put_object_throughput_mb_per_s_percentile_99.9", put_throughput_aggregates.percentile_99_9},
       {"put_object_throughput_mb_per_s_percentile_99.99", put_throughput_aggregates.percentile_99_99},
       {"put_object_throughput_mb_per_s_std_dev", put_throughput_aggregates.standard_deviation},
       {"benchmark_cost_usd",
        static_cast<double>(CalculateBenchmarkCost(result.results_, result.function_instance_mb_size_))},
       {"benchmark_cost_overhead_usd", cost_overhead_ / num_results}},
      {/*aggregated string metrics*/}, result.results_,
      {[&](const BenchmarkItemResult& single_result) {
         const double duration_seconds =
             std::chrono::duration<double>(std::chrono::duration<double, std::milli>(
                                               helper_->ExtractMetric(single_result, kJsonGetObjectDurationKey)))
                 .count();
         return std::make_tuple("get_object_throughput_mb_per_s",
                                result.object_mb_size_ / duration_seconds * result.thread_count_);
       },
       [&](const BenchmarkItemResult& single_result) {
         const double duration_seconds =
             std::chrono::duration<double>(std::chrono::duration<double, std::milli>(
                                               helper_->ExtractMetric(single_result, kJsonPutObjectDurationKey)))
                 .count();
         return std::make_tuple("put_object_throughput_mb_per_s",
                                result.object_mb_size_ / duration_seconds * result.thread_count_);
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("billed_lambda_duration_ms",
                                BenchmarkHelper::ExtractBilledLambdaDuration(single_result));
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("function_cost_usd", static_cast<double>(ExtractFunctionCost(
                                                         single_result, result.function_instance_mb_size_)));
       }},
      {/*extract string metric functions*/});
}

long double NetworkThroughputBenchmark::ExtractFunctionCost(const BenchmarkItemResult& result,
                                                            const size_t function_instance_mb_size) {
  const long double billed_duration = BenchmarkHelper::ExtractBilledLambdaDuration(result);
  const long double function_instance_cost =
      cost_calculator_->CalculateCostLambda(billed_duration, function_instance_mb_size);

  const auto payload_value = Aws::Utils::Json::JsonValue(StreamToString(&result.invoke_result->GetPayload()));
  const auto payload_view = payload_value.View();

  const size_t num_s3_requests_tier_1 = payload_view.GetInteger("num_s3_requests_tier_1");
  const size_t num_s3_requests_tier_2 = payload_view.GetInteger("num_s3_requests_tier_2");
  const size_t s3_storage_used_bytes = payload_view.GetInt64("s3_storage_used_bytes");

  const long double s3_request_cost =
      cost_calculator_->CalculateCostS3Requests(num_s3_requests_tier_1, num_s3_requests_tier_2);
  const long double s3_storage_cost = cost_calculator_->CalculateCostS3StorageMonthly(s3_storage_used_bytes);

  return function_instance_cost + s3_request_cost + s3_storage_cost;
}

}  // namespace skyrise
