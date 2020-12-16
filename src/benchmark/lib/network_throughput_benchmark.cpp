#include "network_throughput_benchmark.hpp"

#include <algorithm>
#include <numeric>
#include <tuple>

#include <magic_enum.hpp>

#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

const size_t kBatchSize = 100;

NetworkThroughputBenchmark::NetworkThroughputBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                       std::shared_ptr<CostCalculator> cost_calculator,
                                                       const std::vector<size_t>& function_instance_mb_sizes,
                                                       const std::vector<size_t>& object_byte_sizes,
                                                       const std::vector<size_t>& thread_counts,
                                                       const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), ExecuteMode::kWarmSequential, repetition_count,
                       kBatchSize) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto object_byte_size : object_byte_sizes) {
      for (const auto thread_count : thread_counts) {
        if (thread_count * object_byte_size <= MbToByte(function_instance_mb_size) / 2) {
          for (const auto operation_type : {S3OperationType::kRead, S3OperationType::kWrite}) {
            Aws::StringStream function_name;
            function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write") << "S3";

            BenchmarkConfig config(function_name.str(), function_instance_mb_size, repetition_count_ / batch_size_,
                                   execute_mode_);
            config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_byte_size, thread_count,
                                                operation_type, repetition_count_ / batch_size_));
            configs_.emplace_back(config, NetworkBenchmarkParameters{function_instance_mb_size, object_byte_size,
                                                                     thread_count, operation_type});
          }
        }
      }
    }
  }
}

Aws::Utils::Json::JsonValue NetworkThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkThroughputBenchmark/" << parameters.function_instance_mb_size_ << "FunctionInstanceMB/"
                 << ByteToMb(parameters.object_byte_size_) << "ObjectMB/" << parameters.thread_count_ << "Threads/"
                 << magic_enum::enum_name(parameters.operation_type_);

  const auto batched_runs = GenerateBatchedSubResultOutput(
      result, benchmark_name.str(), parameters.function_instance_mb_size_, "duration_seconds", [&](const double value) {
        return std::chrono::duration<double>(std::chrono::duration<double, std::milli>(value)).count();
      });

  const auto seconds_durations = ExtractValuesFromBatchedSubResults(batched_runs, "duration_seconds");

  std::vector<double> throughputs;
  throughputs.reserve(seconds_durations.size());

  std::transform(seconds_durations.cbegin(), seconds_durations.cend(), std::back_inserter(throughputs),
                 [&](const double seconds_duration) {
                   return ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ / seconds_duration;
                 });

  const auto aggregates = BenchmarkHelper::CalculateAggregates(throughputs);

  auto output_json = BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"throughput_mb_per_s_minimum", aggregates.maximum},
       {"throughput_mb_per_s_maximum", aggregates.minimum},
       {"throughput_mb_per_s_average", aggregates.average},
       {"throughput_mb_per_s_median", aggregates.median},
       {"throughput_mb_per_s_percentile_0.01", aggregates.percentile_0_01},
       {"throughput_mb_per_s_percentile_0.1", aggregates.percentile_0_1},
       {"throughput_mb_per_s_percentile_1", aggregates.percentile_1},
       {"throughput_mb_per_s_percentile_10", aggregates.percentile_10},
       {"benchmark_cost_usd",
        static_cast<double>(CalculateBenchmarkCost(result, parameters.function_instance_mb_size_))},
       {"benchmark_cost_overhead_usd", cost_overhead_ / configs_.size()}},
      {/*aggregated string metrics*/}, std::make_shared<std::vector<BenchmarkItemResult>>(), {},
      {/*extract string metric functions*/});

  return output_json.WithArray("runs", batched_runs);
}

}  // namespace skyrise
