#include "network_latency_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include <magic_enum.hpp>

#include "utils/costs/pricing.hpp"

namespace skyrise {

NetworkLatencyBenchmark::NetworkLatencyBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                 std::shared_ptr<CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_sizes,
                                                 const ExecuteMode execute_mode, const size_t num_iterations)
    : helper_(std::move(helper)),
      cost_calculator_(std::move(cost_calculator)),
      function_instance_sizes_(function_instance_sizes),
      execute_mode_(execute_mode),
      num_iterations_(num_iterations),
      cost_overhead_(0) {}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> NetworkLatencyBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  Setup();

  std::vector<std::shared_ptr<std::vector<BenchmarkItemResult>>> benchmark_results;

  for (size_t i = 0; i < benchmark_configs_.size(); i++) {
    const auto payloads = GeneratePayloads(function_instance_sizes_[i]);
    benchmark_configs_[i].SetPayloads(payloads);

    benchmark_results.emplace_back(benchmark_runner->RunConfig(benchmark_configs_[i]));
  }

  Teardown();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_output(benchmark_configs_.size());

  for (size_t i = 0; i < benchmark_results.size(); i++) {
    benchmark_output[i] = GenerateResultOutput(benchmark_results[i], function_instance_sizes_[i]);
  }

  return benchmark_output;
}

void NetworkLatencyBenchmark::Setup() {
  cost_overhead_ += helper_->CreateS3BucketIfNotExists(kReadBucket);
  cost_overhead_ += helper_->CreateS3BucketIfNotExists(kWriteBucket);

  cost_overhead_ += helper_->EmptyS3Bucket(kReadBucket);
  cost_overhead_ += helper_->EmptyS3Bucket(kWriteBucket);

  if (execute_mode_ == ExecuteMode::ColdSequential || execute_mode_ == ExecuteMode::WarmSequential) {
    cost_overhead_ += helper_->UploadObjectToS3Bucket(
        kReadBucket, kObjectKey, BenchmarkHelper::GenerateRandomObject(kObjectSizeBytes), kObjectSizeBytes);
  } else {
    for (size_t i = 0; i < num_iterations_; i++) {
      cost_overhead_ +=
          helper_->UploadObjectToS3Bucket(kReadBucket, kObjectKey + "-" + std::to_string(i),
                                          BenchmarkHelper::GenerateRandomObject(kObjectSizeBytes), kObjectSizeBytes);
    }
  }

  for (const auto& function_instance_size : function_instance_sizes_) {
    benchmark_configs_.emplace_back(kFunctionName, function_instance_size, num_iterations_, execute_mode_);
  }
}

void NetworkLatencyBenchmark::Teardown() {
  cost_overhead_ += helper_->EmptyS3Bucket(kReadBucket);
  cost_overhead_ += helper_->EmptyS3Bucket(kWriteBucket);
}

long double NetworkLatencyBenchmark::CalculateBenchmarkCost(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const size_t function_instance_size) {
  std::vector<long double> function_costs;
  std::transform(
      result->cbegin(), result->cend(), std::back_inserter(function_costs),
      [&](const BenchmarkItemResult& result) { return ExtractFunctionCost(result, function_instance_size); });
  const long double benchmark_cost = std::accumulate(function_costs.cbegin(), function_costs.cend(), 0.0L);

  return benchmark_cost;
}

std::vector<std::shared_ptr<Aws::IOStream>> NetworkLatencyBenchmark::GeneratePayloads(
    const size_t function_instance_size) {
  const bool is_parallel =
      !(execute_mode_ == ExecuteMode::ColdSequential || execute_mode_ == ExecuteMode::WarmSequential);

  std::vector<std::shared_ptr<Aws::IOStream>> payloads;

  for (size_t i = 0; i < num_iterations_; i++) {
    const Aws::String object_read_key = is_parallel ? kObjectKey + "-" + std::to_string(i) : kObjectKey;
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> object_read_key_arr(1);
    object_read_key_arr[0] = Aws::Utils::Json::JsonValue().AsString(object_read_key);

    const Aws::String object_write_key =
        kObjectKey + "-" + std::to_string(function_instance_size) + "MB-" + std::to_string(i);
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> object_write_key_arr(1);
    object_write_key_arr[0] = Aws::Utils::Json::JsonValue().AsString(object_write_key);

    const auto payload_value = Aws::Utils::Json::JsonValue()
                                   .WithString("s3_bucket_read", kReadBucket)
                                   .WithArray("s3_keys_read", object_read_key_arr)
                                   .WithString("s3_bucket_write", kWriteBucket)
                                   .WithArray("s3_keys_write", object_write_key_arr);

    const auto payload_view = payload_value.View();
    payloads.emplace_back(std::make_shared<Aws::StringStream>(payload_view.WriteCompact()));
  }
  return payloads;
}

Aws::Utils::Json::JsonValue NetworkLatencyBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const size_t function_instance_size) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkLatencyBenchmark/";
  benchmark_name << magic_enum::enum_name(execute_mode_) << "/" << function_instance_size << "MB";

  const auto get_latency_aggregates =
      BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& result) {
        return BenchmarkHelper::ExtractMetric(result, kJsonGetObjectDurationKey);
      });
  const auto put_latency_aggregates =
      BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& result) {
        return BenchmarkHelper::ExtractMetric(result, kJsonPutObjectDurationKey);
      });

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"get_object_latency_average", get_latency_aggregates.average},
       {"get_object_latency_minimum", get_latency_aggregates.minimum},
       {"get_object_latency_median", get_latency_aggregates.median},
       {"get_object_latency_maximum", get_latency_aggregates.maximum},
       {"get_object_latency_percentile_90", get_latency_aggregates.percentile_90},
       {"get_object_latency_percentile_99", get_latency_aggregates.percentile_99},
       {"get_object_latency_percentile_99.9", get_latency_aggregates.percentile_99_9},
       {"get_object_latency_percentile_99.99", get_latency_aggregates.percentile_99_99},
       {"get_object_latency_std_dev", get_latency_aggregates.standard_deviation},
       {"put_object_latency_average", put_latency_aggregates.average},
       {"put_object_latency_minimum", put_latency_aggregates.minimum},
       {"put_object_latency_median", put_latency_aggregates.median},
       {"put_object_latency_maximum", put_latency_aggregates.maximum},
       {"put_object_latency_percentile_90", put_latency_aggregates.percentile_90},
       {"put_object_latency_percentile_99", put_latency_aggregates.percentile_99},
       {"put_object_latency_percentile_99.9", put_latency_aggregates.percentile_99_9},
       {"put_object_latency_percentile_99.99", put_latency_aggregates.percentile_99_99},
       {"put_object_latency_std_dev", put_latency_aggregates.standard_deviation},
       {"benchmark_cost_usd", CalculateBenchmarkCost(result, function_instance_size)},
       {"benchmark_cost_overhead_usd", cost_overhead_ / function_instance_sizes_.size()}},
      {/*aggregated string metrics*/}, result,
      {[&](const auto& result) {
         return std::make_tuple("get_object_latency_ms",
                                BenchmarkHelper::ExtractMetric(result, kJsonGetObjectDurationKey));
       },
       [&](const auto& result) {
         return std::make_tuple("put_object_latency_ms",
                                BenchmarkHelper::ExtractMetric(result, kJsonPutObjectDurationKey));
       },
       [&](const auto& result) {
         return std::make_tuple("billed_lambda_duration_ms", BenchmarkHelper::ExtractBilledLambdaDuration(result));
       },
       [&](const auto& result) {
         return std::make_tuple("function_cost_usd", ExtractFunctionCost(result, function_instance_size));
       }},
      {/*extract string metric functions*/});
}

long double NetworkLatencyBenchmark::ExtractFunctionCost(const BenchmarkItemResult& result,
                                                         const size_t function_instance_size) {
  const double billed_duration = BenchmarkHelper::ExtractBilledLambdaDuration(result);
  const long double function_instance_cost =
      cost_calculator_->CalculateCostLambda(billed_duration, function_instance_size);

  // TODO(anyone): use stream to string helper PR
  Aws::StringStream payload_stream;
  payload_stream << result.invoke_result->GetPayload().rdbuf();
  const auto payload_value = Aws::Utils::Json::JsonValue(payload_stream.str());
  const auto payload_view = payload_value.View();
  result.invoke_result->GetPayload().seekg(std::ios::beg);

  const size_t num_s3_requests_tier_1 = payload_view.GetInteger("num_s3_requests_tier_1");
  const size_t num_s3_requests_tier_2 = payload_view.GetInteger("num_s3_requests_tier_2");
  const size_t s3_storage_used_bytes = payload_view.GetInt64("s3_storage_used_bytes");

  const long double s3_request_cost =
      cost_calculator_->CalculateCostS3Requests(num_s3_requests_tier_1, num_s3_requests_tier_2);
  const long double s3_storage_cost = cost_calculator_->CalculateCostS3StorageMonthly(s3_storage_used_bytes);

  return function_instance_cost + s3_request_cost + s3_storage_cost;
}

}  // namespace skyrise
