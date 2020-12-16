#include "network_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include "utils/assert.hpp"
#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

NetworkBenchmark::NetworkBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                   std::shared_ptr<CostCalculator> cost_calculator, const ExecuteMode execute_mode,
                                   const size_t repetition_count, const size_t batch_size)
    : helper_(std::move(helper)),
      cost_calculator_(std::move(cost_calculator)),
      execute_mode_(execute_mode),
      repetition_count_(repetition_count),
      batch_size_(batch_size),
      cost_overhead_(0) {}

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
    Assert(!std::get<0>(results[i])->empty(), "The benchmark results must never be empty.");
    results_array[i] = GenerateResultOutput(std::get<0>(results[i]), std::get<1>(results[i]));
  }

  return results_array;
}

void NetworkBenchmark::Setup() {
  cost_overhead_ = 0;

  cost_overhead_ += helper_->CreateS3BucketIfNotExists(kReadBucket);
  cost_overhead_ += helper_->CreateS3BucketIfNotExists(kWriteBucket);

  cost_overhead_ += helper_->EmptyS3Bucket(kReadBucket);
  cost_overhead_ += helper_->EmptyS3Bucket(kWriteBucket);

  const bool is_parallel =
      execute_mode_ != ExecuteMode::kColdSequential && execute_mode_ != ExecuteMode::kWarmSequential;

  for (const auto& [config, parameters] : configs_) {
    if (parameters.operation_type_ == S3OperationType::kRead) {
      for (size_t i = 0; i < parameters.thread_count_; i++) {
        if (is_parallel) {
          for (size_t j = 0; j < repetition_count_; j++) {
            cost_overhead_ += helper_->UploadObjectToS3Bucket(
                kReadBucket, GenerateObjectKey(true, parameters.object_byte_size_, i, j),
                BenchmarkHelper::GenerateRandomObject(parameters.object_byte_size_), parameters.object_byte_size_);
          }
        } else {
          cost_overhead_ += helper_->UploadObjectToS3Bucket(
              kReadBucket, GenerateObjectKey(false, parameters.object_byte_size_, i),
              BenchmarkHelper::GenerateRandomObject(parameters.object_byte_size_), parameters.object_byte_size_);
        }
      }
    }
  }
}

void NetworkBenchmark::Teardown() {
  cost_overhead_ += helper_->EmptyS3Bucket(kReadBucket);
  cost_overhead_ += helper_->EmptyS3Bucket(kWriteBucket);
}

Aws::String NetworkBenchmark::GenerateObjectKey(const bool is_parallel, const size_t objects_byte_size,
                                                const size_t thread_index, const size_t iteration_index) {
  // TODO(d-justen): Employ objext key prefixes as described in
  // https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html

  Aws::StringStream object_key;
  object_key << thread_index << "-" << (is_parallel ? std::to_string(iteration_index) : "") << objects_byte_size << "B-"
             << kObjectKeySuffix;
  return object_key.str();
}

std::vector<std::shared_ptr<Aws::IOStream>> NetworkBenchmark::GeneratePayloads(const size_t function_instance_mb_size,
                                                                               const size_t object_byte_size,
                                                                               const size_t thread_count,
                                                                               const S3OperationType operation_type,
                                                                               const size_t payload_count) {
  const bool is_parallel =
      execute_mode_ != ExecuteMode::kColdSequential && execute_mode_ != ExecuteMode::kWarmSequential;

  std::vector<std::shared_ptr<Aws::IOStream>> payloads;
  payloads.reserve(payload_count);

  for (size_t i = 0; i < payload_count; i++) {
    Aws::Utils::Array<Aws::String> object_keys(thread_count);

    for (size_t j = 0; j < thread_count; j++) {
      Aws::StringStream object_key;
      object_key << GenerateObjectKey(is_parallel, object_byte_size, j, i);

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

long double NetworkBenchmark::ExtractFunctionCost(const BenchmarkItemResult& result,
                                                  const size_t function_instance_mb_size) {
  const double billed_duration = BenchmarkHelper::ExtractBilledLambdaDuration(result);
  const long double function_instance_cost =
      cost_calculator_->CalculateCostLambda(billed_duration, function_instance_mb_size);

  const auto payload_value = Aws::Utils::Json::JsonValue(StreamToString(&result.invoke_result->GetPayload()));
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

Aws::Utils::Array<Aws::Utils::Json::JsonValue> NetworkBenchmark::GenerateBatchedSubResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const Aws::String& benchmark_name,
    const size_t function_instance_mb_size, const Aws::String& metric_name,
    const std::function<double(const double)>& process_value) {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> batched_runs(result->size());

  for (size_t i = 0; i < result->size(); i++) {
    Aws::Utils::Json::JsonValue result_value(StreamToString(&(*result)[i].invoke_result->GetPayload()));
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
            .WithDouble("billed_lambda_duration_ms", BenchmarkHelper::ExtractBilledLambdaDuration((*result)[i]))
            .WithDouble("function_cost_usd",
                        static_cast<double>(ExtractFunctionCost((*result)[i], function_instance_mb_size)));
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
