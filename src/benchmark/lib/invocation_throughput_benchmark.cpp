#include "invocation_throughput_benchmark.hpp"

#include <algorithm>
#include <array>

#include "benchmark_result_aggregate.hpp"
#include "utils/string.hpp"

namespace skyrise {

InvocationThroughputBenchmark::InvocationThroughputBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                                                             const std::vector<size_t>& function_instance_mb_sizes,
                                                             const std::vector<size_t>& invocation_counts,
                                                             const std::vector<size_t>& function_payload_byte_sizes,
                                                             const size_t repetition_count)
    : Benchmark(std::move(cost_calculator)) {
  std::array<UseEventQueue, 2> use_event_queues{UseEventQueue::kYes, UseEventQueue::kNo};

  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() *
                             function_payload_byte_sizes.size() * use_event_queues.size());

  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto invocation_count : invocation_counts) {
      for (const auto function_payload_byte_size : function_payload_byte_sizes) {
        for (const auto use_event_queue : use_event_queues) {
          BenchmarkConfig config(kFunctionName, function_instance_mb_size, repetition_count, invocation_count,
                                 WarmUp::kDefault, UseOneFunctionPerRepetition::kNo, use_event_queue);

          if (function_payload_byte_size > 0) {
            config.SetOnePayloadForAllFunctions(BenchmarkHelper::GenerateRandomObject(function_payload_byte_size));
          }

          benchmark_configs_.emplace_back(
              InvocationThroughputBenchmarkParameters{function_instance_mb_size, invocation_count,
                                                      function_payload_byte_size, repetition_count, use_event_queue},
              config);
        }
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> InvocationThroughputBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<BenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunConfig(benchmark_config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        InvocationThroughputBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue InvocationThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result,
    const InvocationThroughputBenchmarkParameters& benchmark_parameters) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "InvocationThroughputBenchmark/" << benchmark_parameters.function_instance_mb_size << "/"
                 << benchmark_parameters.invocation_count << "/" << benchmark_parameters.function_payload_byte_size
                 << "/" << benchmark_parameters.repetition_count << "/"
                 << (benchmark_parameters.use_event_queue == UseEventQueue::kYes ? "Yes" : "No");

  const auto& invocation_results = benchmark_result->GetInvocationResults();

  std::vector<double> invocation_throughputs;
  invocation_throughputs.reserve(invocation_results.size());

  for (const auto& invocation_result : invocation_results) {
    auto min_start_time = std::chrono::system_clock::time_point::max();
    auto max_end_time = std::chrono::system_clock::time_point::min();

    for (const auto& item_result : invocation_result) {
      min_start_time = std::min(min_start_time, item_result.second.start_point_);
      max_end_time = std::max(max_end_time, item_result.second.end_point_);
    }

    const double duration = std::chrono::duration<double>(max_end_time - min_start_time).count();
    const double throughput = benchmark_parameters.invocation_count / duration;

    invocation_throughputs.emplace_back(throughput);
  }

  const BenchmarkResultAggregate aggregates(invocation_throughputs);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"invocation_throughput_functions_per_s_minimum", aggregates.GetMinimum()},
       {"invocation_throughput_functions_per_s_maximum", aggregates.GetMaximum()},
       {"invocation_throughput_functions_per_s_average", aggregates.GetAverage()},
       {"invocation_throughput_functions_per_s_median", aggregates.GetMedian()},
       {"invocation_throughput_functions_per_s_percentile_0.01", aggregates.GetPercentile(0.01)},
       {"invocation_throughput_functions_per_s_percentile_0.1", aggregates.GetPercentile(0.1)},
       {"invocation_throughput_functions_per_s_percentile_1", aggregates.GetPercentile(1)},
       {"invocation_throughput_functions_per_s_percentile_10", aggregates.GetPercentile(10)},
       {"invocation_throughput_functions_per_s_std_dev", aggregates.GetStandardDeviation()},
       {"benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                  benchmark_result, benchmark_parameters.function_instance_mb_size))},
       {"warm_up_cost_usd", static_cast<double>(benchmark_result->GetOverallFunctionWarmUpCost())}},
      {/*aggregated string metrics*/}, benchmark_result,
      {[&](const InvocationResult& item_result) {
         return std::make_tuple(
             "duration", std::chrono::duration<double>(item_result.end_point_ - item_result.start_point_).count());
       },
       [&](const InvocationResult& item_result) {
         return std::make_tuple("function_cost_usd",
                                ExtractFunctionCost(item_result, benchmark_parameters.function_instance_mb_size));
       }},
      {/*extract string metric functions*/}, {/*extract object metric functions*/});
}

}  // namespace skyrise
