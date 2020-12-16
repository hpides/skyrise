#include "network_throughput_parallel_benchmark.hpp"

#include <algorithm>
#include <numeric>
#include <regex>
#include <tuple>
#include <unordered_map>

#include <magic_enum.hpp>

#include "utils/literal.hpp"
#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

const size_t kBatchSize = 1;
// TODO(d-justen): Change to the best performing parameters found by NetworkThroughputBenchmark
const size_t kFunctionInstanceMbSize = 3008;
const size_t kObjectByteSize = 16_MB;
const size_t kThreadCount = 4;

NetworkThroughputParallelBenchmark::NetworkThroughputParallelBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                                       std::shared_ptr<CostCalculator> cost_calculator,
                                                                       const std::vector<size_t>& invocation_counts,
                                                                       const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), ExecuteMode::kWarmParallel,
                       *std::max_element(invocation_counts.cbegin(), invocation_counts.cend()), kBatchSize) {
  for (const auto operation_type : {S3OperationType::kRead, S3OperationType::kWrite}) {
    Aws::StringStream function_name;
    function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write") << "S3";

    for (const auto invocation_count : invocation_counts) {
      BenchmarkConfig config(function_name.str(), kFunctionInstanceMbSize, invocation_count, execute_mode_,
                             repetition_count, std::vector<std::function<void()>>(repetition_count, [] {}));
      config.SetPayloads(
          GeneratePayloads(kFunctionInstanceMbSize, kObjectByteSize, kThreadCount, operation_type, invocation_count));
      configs_.emplace_back(
          config, NetworkBenchmarkParameters{kFunctionInstanceMbSize, kObjectByteSize, kThreadCount, operation_type});
    }
  }
}

Aws::Utils::Json::JsonValue NetworkThroughputParallelBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkThroughputParallelBenchmark/" << parameters.function_instance_mb_size_
                 << "FunctionInstanceMB/" << ByteToMb(parameters.object_byte_size_) << "ObjectSizeMB/"
                 << parameters.thread_count_ << "Threads/" << magic_enum::enum_name(parameters.operation_type_);

  std::unordered_map<size_t, std::shared_ptr<std::vector<BenchmarkItemResult>>> repetition_map;

  // TODO(anyone): Wrap the repetitions in a richer structure on the BenchmarkRunner side
  const std::regex repetition_regex("repetition-(\\d+)");

  for (const auto& benchmark_item_result : *result) {
    std::smatch repetition_match;
    std::regex_search(benchmark_item_result.invocation_id, repetition_match, repetition_regex);
    const size_t repetition = std::stoi(repetition_match[1]);

    if (repetition_map.find(repetition) == repetition_map.cend()) {
      repetition_map[repetition] = std::make_shared<std::vector<BenchmarkItemResult>>();
    }

    repetition_map[repetition]->emplace_back(benchmark_item_result);
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> parallel_runs(repetition_map.size());
  std::vector<double> seconds_durations;
  seconds_durations.reserve(repetition_map.size());

  for (const auto& [repetition, benchmark_item_results] : repetition_map) {
    parallel_runs[repetition] =
        GenerateSubResultOutput(benchmark_name.str(), repetition, benchmark_item_results, parameters);
    seconds_durations.emplace_back(parallel_runs[repetition].View().GetDouble("duration_seconds"));
  }

  std::vector<double> throughputs;
  throughputs.reserve(seconds_durations.size());

  std::transform(seconds_durations.cbegin(), seconds_durations.cend(), std::back_inserter(throughputs),
                 [&](const double seconds_duration) {
                   return ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ *
                          repetition_map[0]->size() / seconds_duration;
                 });

  const auto aggregates = BenchmarkHelper::CalculateAggregates(throughputs);

  auto output_json = BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"throughput_parallel_mb_per_s_minimum", aggregates.maximum},
       {"throughput_parallel_mb_per_s_maximum", aggregates.minimum},
       {"throughput_parallel_mb_per_s_average", aggregates.average},
       {"throughput_parallel_mb_per_s_median", aggregates.median},
       {"throughput_parallel_mb_per_s_percentile_0.01", aggregates.percentile_0_01},
       {"throughput_parallel_mb_per_s_percentile_0.1", aggregates.percentile_0_1},
       {"throughput_parallel_mb_per_s_percentile_1", aggregates.percentile_1},
       {"throughput_parallel_mb_per_s_percentile_10", aggregates.percentile_10},
       {"benchmark_cost_usd",
        static_cast<double>(CalculateBenchmarkCost(result, parameters.function_instance_mb_size_))},
       {"benchmark_cost_overhead_usd", cost_overhead_ / configs_.size()}},
      {/*aggregated string metrics*/}, std::make_shared<std::vector<BenchmarkItemResult>>(), {},
      {/*extract string metric functions*/});

  return output_json.WithArray("runs", parallel_runs);
}

Aws::Utils::Json::JsonValue NetworkThroughputParallelBenchmark::GenerateSubResultOutput(
    const Aws::String& benchmark_name, const size_t repetition,
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const NetworkBenchmarkParameters& parameters) {
  std::vector<double> seconds_durations;

  std::transform(
      result->cbegin(), result->cend(), std::back_inserter(seconds_durations),
      [&](const BenchmarkItemResult& single_result) {
        Aws::Utils::Json::JsonValue result_value(StreamToString(&single_result.invoke_result->GetPayload()));
        const auto duration_views = result_value.View().GetArray("ms_durations");

        return std::chrono::duration<double>(std::chrono::duration<double, std::milli>(duration_views[0].AsDouble()))
            .count();
      });

  const double max_duration_seconds = *std::max_element(seconds_durations.cbegin(), seconds_durations.cend());
  const auto throughput_mb_per_second = static_cast<double>(
      ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ * result->size() / max_duration_seconds);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name + "/" + std::to_string(repetition),
      {{"throughput_parallel_mb_per_s", throughput_mb_per_second},
       {"duration_seconds", max_duration_seconds},
       {"function_cost_aggregated_usd",
        static_cast<double>(CalculateBenchmarkCost(result, parameters.function_instance_mb_size_))}},
      {/*aggregated string metrics*/}, result,
      {[&](const BenchmarkItemResult& single_result) {
         Aws::Utils::Json::JsonValue result_value(StreamToString(&single_result.invoke_result->GetPayload()));
         const auto duration_views = result_value.View().GetArray("ms_durations");
         const double duration_seconds =
             std::chrono::duration<double>(std::chrono::duration<double, std::milli>(duration_views[0].AsDouble()))
                 .count();
         return std::make_tuple("throughput_mb_per_s",
                                ByteToMb(parameters.object_byte_size_) / duration_seconds * parameters.thread_count_);
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("billed_lambda_duration_ms",
                                BenchmarkHelper::ExtractBilledLambdaDuration(single_result));
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("function_cost_usd", static_cast<double>(ExtractFunctionCost(
                                                         single_result, parameters.function_instance_mb_size_)));
       }},
      {/*extract string metric functions*/});
}

}  // namespace skyrise
