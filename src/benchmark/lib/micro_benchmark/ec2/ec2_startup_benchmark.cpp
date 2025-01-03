#include "ec2_startup_benchmark.hpp"

#include <magic_enum/magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "ec2_benchmark_output.hpp"
#include "ec2_benchmark_runner.hpp"
#include "utils/assert.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "ec2StartupBenchmark";

}  // namespace

Ec2StartupBenchmark::Ec2StartupBenchmark(const std::vector<Ec2InstanceType>& instance_types,
                                         const std::vector<size_t>& concurrent_instance_counts, size_t repetition_count,
                                         const bool warmup_flag) {
  benchmark_configs_.reserve(instance_types.size() * concurrent_instance_counts.size());

  const Ec2WarmupType warmup_type = warmup_flag ? Ec2WarmupType::kDefault : Ec2WarmupType::kNone;

  for (const auto instance_type : instance_types) {
    for (const auto concurrent_instance_count : concurrent_instance_counts) {
      benchmark_configs_.emplace_back(
          std::make_shared<Ec2BenchmarkConfig>(instance_type, concurrent_instance_count, repetition_count, warmup_type),
          Ec2StartupBenchmarkParameters{{instance_type, concurrent_instance_count, repetition_count}, warmup_type});
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> Ec2StartupBenchmark::Run(
    const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) {
  const auto ec2_benchmark_runner = std::dynamic_pointer_cast<Ec2BenchmarkRunner>(benchmark_runner);
  Assert(ec2_benchmark_runner, "Ec2StartupBenchmark needs an Ec2BenchmarkRunner to run.");

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results_.push_back(ec2_benchmark_runner->RunEc2Config(benchmark_config.first));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> result_outputs(benchmark_results_.size());

  for (size_t i = 0; i < benchmark_results_.size(); ++i) {
    result_outputs[i] = GenerateResultOutput(benchmark_results_[i], benchmark_configs_[i].second);
  }

  return result_outputs;
}

Aws::Utils::Json::JsonValue Ec2StartupBenchmark::GenerateResultOutput(
    const std::shared_ptr<Ec2BenchmarkResult>& result, const Ec2StartupBenchmarkParameters& parameters) const {
  auto benchmark_output = Ec2BenchmarkOutput(Name(), parameters.base_parameters, result);

  benchmark_output
      .WithStringArgument("instance_type", std::string(magic_enum::enum_name(parameters.base_parameters.instance_type)))
      .WithInt64Argument("concurrent_instance_count", parameters.base_parameters.concurrent_instance_count)
      .WithInt64Argument("repetition_count", parameters.base_parameters.repetition_count)
      .WithStringArgument("warmup_type", std::string(magic_enum::enum_name(parameters.warmup_type)))
      .WithObjectRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition) {
        std::vector<double> coldstart_latencies;
        coldstart_latencies.reserve(repetition.invocation_results.size());
        for (const auto& [instance_id, transition_duration] : repetition.invocation_results) {
          coldstart_latencies.push_back(transition_duration.instance_transitions.coldstart_duration_ms);
        }

        const BenchmarkResultAggregate coldstart_latency_aggregate(coldstart_latencies, 0);
        auto metrics =
            Aws::Utils::Json::JsonValue()
                .WithDouble("coldstart_latency_ms_average", coldstart_latency_aggregate.GetAverage())
                .WithDouble("coldstart_latency_ms_minimum", coldstart_latency_aggregate.GetMinimum())
                .WithDouble("coldstart_latency_ms_maximum", coldstart_latency_aggregate.GetMaximum())
                .WithDouble("coldstart_latency_ms_median", coldstart_latency_aggregate.GetMedian())
                .WithDouble("coldstart_latency_ms_std_dev", coldstart_latency_aggregate.GetStandardDeviation())
                .WithDouble("coldstart_latency_ms_percentile_90", coldstart_latency_aggregate.GetPercentile(90))
                .WithDouble("coldstart_latency_ms_percentile_99", coldstart_latency_aggregate.GetPercentile(99))
                .WithDouble("coldstart_latency_ms_percentile_99_9", coldstart_latency_aggregate.GetPercentile(99.9))
                .WithDouble("coldstart_latency_ms_percentile_99_99", coldstart_latency_aggregate.GetPercentile(99.99));
        return std::make_tuple("coldstart_metrics", metrics);
      });

  if (parameters.warmup_type == Ec2WarmupType::kDefault) {
    benchmark_output.WithObjectRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition) {
      std::vector<double> stop_latencies;
      std::vector<double> warmstart_latencies;
      stop_latencies.reserve(repetition.invocation_results.size());
      warmstart_latencies.reserve(repetition.invocation_results.size());

      for (const auto& [instance_id, transition_duration] : repetition.invocation_results) {
        stop_latencies.push_back(transition_duration.instance_transitions.stop_duration_ms.value());
        warmstart_latencies.push_back(transition_duration.instance_transitions.warmstart_duration_ms.value());
      }

      const BenchmarkResultAggregate stop_latency_aggregate(stop_latencies, 0);
      const BenchmarkResultAggregate warmstart_latency_aggregate(warmstart_latencies, 0);
      auto metrics =
          Aws::Utils::Json::JsonValue()
              .WithDouble("stop_latency_ms_average", stop_latency_aggregate.GetAverage())
              .WithDouble("stop_latency_ms_minimum", stop_latency_aggregate.GetMinimum())
              .WithDouble("stop_latency_ms_maximum", stop_latency_aggregate.GetMaximum())
              .WithDouble("stop_latency_ms_median", stop_latency_aggregate.GetMedian())
              .WithDouble("stop_latency_ms_std_dev", stop_latency_aggregate.GetStandardDeviation())
              .WithDouble("stop_latency_ms_percentile_90", stop_latency_aggregate.GetPercentile(90))
              .WithDouble("stop_latency_ms_percentile_99", stop_latency_aggregate.GetPercentile(99))
              .WithDouble("stop_latency_ms_percentile_99_9", stop_latency_aggregate.GetPercentile(99.9))
              .WithDouble("stop_latency_ms_percentile_99_99", stop_latency_aggregate.GetPercentile(99.99))
              .WithDouble("warmstart_latency_ms_average", warmstart_latency_aggregate.GetAverage())
              .WithDouble("warmstart_latency_ms_minimum", warmstart_latency_aggregate.GetMinimum())
              .WithDouble("warmstart_latency_ms_maximum", warmstart_latency_aggregate.GetMaximum())
              .WithDouble("warmstart_latency_ms_median", warmstart_latency_aggregate.GetMedian())
              .WithDouble("warmstart_latency_ms_std_dev", warmstart_latency_aggregate.GetStandardDeviation())
              .WithDouble("warmstart_latency_ms_percentile_90", warmstart_latency_aggregate.GetPercentile(90))
              .WithDouble("warmstart_latency_ms_percentile_99", warmstart_latency_aggregate.GetPercentile(99))
              .WithDouble("warmstart_latency_ms_percentile_99_9", warmstart_latency_aggregate.GetPercentile(99.9))
              .WithDouble("warmstart_latency_ms_percentile_99_99", warmstart_latency_aggregate.GetPercentile(99.99));
      return std::make_tuple("warmstart_metrics", metrics);
    });
  }

  benchmark_output.WithObjectRepetitionMetric([&](const Ec2BenchmarkRepetitionResult& repetition) {
    std::vector<double> termination_latencies;
    termination_latencies.reserve(repetition.invocation_results.size());
    for (const auto& [instance_id, transition_duration] : repetition.invocation_results) {
      termination_latencies.push_back(transition_duration.instance_transitions.termination_duration_ms.value());
    }

    const BenchmarkResultAggregate termination_latency_aggregate(termination_latencies, 0);
    auto metrics =
        Aws::Utils::Json::JsonValue()
            .WithDouble("termination_latency_ms_average", termination_latency_aggregate.GetAverage())
            .WithDouble("termination_latency_ms_minimum", termination_latency_aggregate.GetMinimum())
            .WithDouble("termination_latency_ms_maximum", termination_latency_aggregate.GetMaximum())
            .WithDouble("termination_latency_ms_median", termination_latency_aggregate.GetMedian())
            .WithDouble("termination_latency_ms_std_dev", termination_latency_aggregate.GetStandardDeviation())
            .WithDouble("termination_latency_ms_percentile_90", termination_latency_aggregate.GetPercentile(90))
            .WithDouble("termination_latency_ms_percentile_99", termination_latency_aggregate.GetPercentile(99))
            .WithDouble("termination_latency_ms_percentile_99_9", termination_latency_aggregate.GetPercentile(99.9))
            .WithDouble("termination_latency_ms_percentile_99_99", termination_latency_aggregate.GetPercentile(99.99));
    return std::make_tuple("termination_metrics", metrics);
  });

  return benchmark_output.Build();
}

const Aws::String& Ec2StartupBenchmark::Name() const { return kName; }

}  // namespace skyrise
