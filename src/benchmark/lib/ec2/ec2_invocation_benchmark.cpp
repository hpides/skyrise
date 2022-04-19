#include "ec2_invocation_benchmark.hpp"

#include <chrono>
#include <sstream>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "ec2_benchmark_runner.hpp"
#include "utils/assert.hpp"

namespace skyrise {

Ec2InvocationBenchmark::Ec2InvocationBenchmark(const std::vector<size_t>& concurrent_invocation_counts,
                                               const std::vector<Ec2InstanceType>& instance_types,
                                               size_t repetition_count) {
  benchmark_configs_.reserve(concurrent_invocation_counts.size() * instance_types.size());

  for (const auto concurrent_invocation_count : concurrent_invocation_counts) {
    for (const auto instance_type : instance_types) {
      benchmark_configs_.emplace_back(
          std::make_shared<Ec2BenchmarkConfig>(instance_type, repetition_count, concurrent_invocation_count),
          Ec2InvocationBenchmarkParameters{concurrent_invocation_count, instance_type, repetition_count});
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> Ec2InvocationBenchmark::Run(
    const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) {
  const auto ec2_benchmark_runner = std::dynamic_pointer_cast<Ec2BenchmarkRunner>(benchmark_runner);
  Assert(ec2_benchmark_runner, "Ec2InvocationBenchmark needs an Ec2BenchmarkRunner to run.");

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results_.push_back(ec2_benchmark_runner->RunEc2Config(benchmark_config.first));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> result_outputs(benchmark_results_.size());

  for (size_t i = 0; i < benchmark_results_.size(); ++i) {
    result_outputs[i] = GenerateResultOutput(benchmark_results_[i], benchmark_configs_[i].second);
  }

  return result_outputs;
}

Aws::Utils::Json::JsonValue Ec2InvocationBenchmark::GenerateResultOutput(
    const std::shared_ptr<Ec2BenchmarkResult>& benchmark_result, const Ec2InvocationBenchmarkParameters& parameters) {
  std::vector<double> invocation_latencies;
  invocation_latencies.reserve(parameters.concurrent_invocation_count * parameters.repetition_count);

  std::vector<double> invocation_throughputs;
  invocation_throughputs.reserve(parameters.repetition_count);

  std::vector<double> cooldown_latencies;
  cooldown_latencies.reserve(parameters.concurrent_invocation_count * parameters.repetition_count);

  for (const auto& repetition : benchmark_result->GetRepetitions()) {
    const double duration_seconds =
        std::chrono::duration<double>(std::chrono::duration<double, std::milli>(repetition.duration_ms.value_or(0)))
            .count();
    invocation_throughputs.push_back(repetition.launch_durations.size() / duration_seconds);

    for (const auto& launch_duration : repetition.launch_durations) {
      invocation_latencies.push_back(launch_duration.second.duration_ms);
      cooldown_latencies.push_back(launch_duration.second.cooldown_ms.value());
    }
  }

  const BenchmarkResultAggregate latency_aggregate(invocation_latencies);
  const BenchmarkResultAggregate throughput_aggregate(invocation_throughputs);
  const BenchmarkResultAggregate cooldown_aggregate(cooldown_latencies);

  auto json_output =
      Aws::Utils::Json::JsonValue()
          .WithString("name", "ec2_invocation_benchmark")
          .WithObject("arguments",
                      Aws::Utils::Json::JsonValue()
                          .WithInteger("concurrent_invocation_count", parameters.concurrent_invocation_count)
                          .WithString("instance_type", std::string(magic_enum::enum_name(parameters.instance_type)))
                          .WithInteger("repetition_count", parameters.repetition_count))
          .WithObject(
              "metrics",
              Aws::Utils::Json::JsonValue()
                  .WithDouble("latency_ms_minimum", latency_aggregate.GetMinimum())
                  .WithDouble("latency_ms_maximum", latency_aggregate.GetMaximum())
                  .WithDouble("latency_ms_median", latency_aggregate.GetMedian())
                  .WithDouble("latency_ms_std_dev", latency_aggregate.GetStandardDeviation())
                  .WithDouble("latency_ms_percentile_90", latency_aggregate.GetPercentile(90))
                  .WithDouble("latency_ms_percentile_99", latency_aggregate.GetPercentile(99))
                  .WithDouble("latency_ms_percentile_99_9", latency_aggregate.GetPercentile(99.9))
                  .WithDouble("throughput_instances_per_s_median", throughput_aggregate.GetMedian())
                  .WithDouble("throughput_instances_per_s_percentile_10", throughput_aggregate.GetPercentile(10))
                  .WithDouble("throughput_instances_per_s_percentile_1", throughput_aggregate.GetPercentile(1))
                  .WithDouble("throughput_instances_per_s_percentile_0_1", throughput_aggregate.GetPercentile(0.1))
                  .WithDouble("cooldown_ms_minimum", cooldown_aggregate.GetMinimum())
                  .WithDouble("cooldown_ms_maximum", cooldown_aggregate.GetMaximum())
                  .WithDouble("cooldown_ms_median", cooldown_aggregate.GetMedian())
                  .WithDouble("cooldown_ms_std_dev", cooldown_aggregate.GetStandardDeviation())
                  .WithDouble("cooldown_ms_percentile_90", cooldown_aggregate.GetPercentile(90))
                  .WithDouble("cooldown_ms_percentile_99", cooldown_aggregate.GetPercentile(99))
                  .WithDouble("cooldown_ms_percentile_99_9", cooldown_aggregate.GetPercentile(99.9)));

  return json_output;
}

}  // namespace skyrise
