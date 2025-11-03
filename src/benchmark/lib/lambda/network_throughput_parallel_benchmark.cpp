#include "network_throughput_parallel_benchmark.hpp"

#include <numeric>
#include <regex>
#include <tuple>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

NetworkThroughputParallelBenchmark::NetworkThroughputParallelBenchmark(
    std::shared_ptr<const BenchmarkHelper> helper, std::shared_ptr<const CostCalculator> cost_calculator,
    const std::vector<size_t>& function_instance_mb_sizes, const std::vector<size_t>& object_byte_sizes,
    const std::vector<size_t>& batch_sizes, const std::vector<size_t>& thread_counts,
    const std::vector<size_t>& invocation_counts, const std::vector<size_t>& bucket_counts, const bool enable_reads,
    const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), bucket_counts) {
  std::vector<S3OperationType> operation_types{S3OperationType::kWrite};
  if (enable_reads) {
    operation_types.emplace_back(S3OperationType::kRead);
  }

  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto object_byte_size : object_byte_sizes) {
      for (const auto batch_size : batch_sizes) {
        for (const auto thread_count : thread_counts) {
          for (const auto invocation_count : invocation_counts) {
            for (const auto bucket_count : bucket_counts) {
              for (const auto operation_type : operation_types) {
                Aws::StringStream function_name;
                function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write")
                              << "S3";

                const auto config =
                    std::make_shared<LambdaBenchmarkConfig>(function_name.str(), function_instance_mb_size,
                                                            repetition_count, invocation_count, WarmUp::kDefault);
                NetworkBenchmarkParameters parameters{
                    function_instance_mb_size, object_byte_size, batch_size,       thread_count,
                    invocation_count,          bucket_count,     repetition_count, operation_type};
                config->SetPayloads(GeneratePayloads(parameters));

                benchmark_configs_.emplace_back(parameters, config);
              }
            }
          }
        }
      }
    }
  }
}

Aws::Utils::Json::JsonValue NetworkThroughputParallelBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const NetworkBenchmarkParameters& benchmark_parameters) {
  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> throughputs;
  throughputs.reserve(benchmark_repetitions.size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    throughputs.emplace_back(ByteToMb(benchmark_parameters.object_byte_size) * benchmark_parameters.thread_count *
                             benchmark_parameters.batch_size * benchmark_repetition.GetInvokeResults().size() /
                             benchmark_repetition.GetDurationSeconds());
  }

  const BenchmarkResultAggregate aggregate(throughputs);

  return LambdaBenchmarkOutput("network_throughput_parallel_benchmark", benchmark_result)
      .WithInt64Argument("function_instance_mb_size", benchmark_parameters.function_instance_mb_size)
      .WithInt64Argument("object_byte_size", benchmark_parameters.object_byte_size)
      .WithInt64Argument("batch_size", benchmark_parameters.batch_size)
      .WithInt64Argument("thread_count", benchmark_parameters.thread_count)
      .WithInt64Argument("invocation_count", benchmark_parameters.invocation_count)
      .WithInt64Argument("bucket_count", benchmark_parameters.bucket_count)
      .WithInt64Argument("repetition_count", benchmark_parameters.repetition_count)
      .WithStringArgument("operation_type", std::string(magic_enum::enum_name(benchmark_parameters.operation_type)))
      .WithDoubleMetric("throughput_parallel_mb_per_s_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("throughput_parallel_mb_per_s_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("throughput_parallel_mb_per_s_average", aggregate.GetAverage())
      .WithDoubleMetric("throughput_parallel_mb_per_s_median", aggregate.GetMedian())
      .WithDoubleMetric("throughput_parallel_mb_per_s_percentile_0.01", aggregate.GetPercentile(0.01))
      .WithDoubleMetric("throughput_parallel_mb_per_s_percentile_0.1", aggregate.GetPercentile(0.1))
      .WithDoubleMetric("throughput_parallel_mb_per_s_percentile_1", aggregate.GetPercentile(1))
      .WithDoubleMetric("throughput_parallel_mb_per_s_percentile_10", aggregate.GetPercentile(10))
      .WithDoubleMetric("throughput_parallel_mb_per_s_std_dev", aggregate.GetStandardDeviation())
      .WithDoubleMetric("benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                                  benchmark_result, benchmark_parameters.function_instance_mb_size)))
      .WithDoubleInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("function_cost_usd",
                               ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size));
      })
      .WithDoubleInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("billed_lambda_duration_ms", invoke_result.GetLogResult()->GetBilledDurationMs());
      })
      .WithDoubleInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        const auto duration_views = invoke_result.GetResponseBody().GetArray("ms_durations");
        const double duration_seconds =
            std::chrono::duration<double>(std::chrono::duration<double, std::milli>(duration_views[0].AsDouble()))
                .count();
        return std::make_tuple("throughput_mb_per_s", ByteToMb(benchmark_parameters.object_byte_size) /
                                                          duration_seconds * benchmark_parameters.thread_count);
      })
      .WithObjectInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        const auto ms_durations = invoke_result.GetResponseBody().GetArray("ms_durations");

        Aws::Utils::Array<Aws::Utils::Json::JsonValue> duration_seconds(ms_durations.GetLength());

        for (size_t i = 0; i < ms_durations.GetLength(); ++i) {
          duration_seconds[i] = Aws::Utils::Json::JsonValue().AsDouble(
              std::chrono::duration<double>(std::chrono::duration<double, std::milli>(ms_durations[i].AsDouble()))
                  .count());
        }

        return std::make_tuple("duration_seconds", Aws::Utils::Json::JsonValue().AsArray(duration_seconds));
      })
      .WithDoubleRepetitionMetric([&](const LambdaBenchmarkRepetition& repetition) {
        const auto throughput = ByteToMb(benchmark_parameters.object_byte_size) * benchmark_parameters.thread_count *
                                benchmark_parameters.batch_size * repetition.GetInvokeResults().size() /
                                repetition.GetDurationSeconds();
        return std::make_tuple("parallel_throughput_mb_per_s", throughput);
      })
      .WithObjectRepetitionMetric([&](const LambdaBenchmarkRepetition& repetition) {
        std::vector<double> throughputs;
        throughputs.reserve(repetition.GetInvokeResults().size());

        for (auto const& invocation : repetition.GetInvokeResults()) {
          const double duration_ms = invocation.GetResponseBody().GetArray("ms_durations")[0].AsDouble();
          const double duration_seconds =
              std::chrono::duration<double>(std::chrono::duration<double, std::milli>(duration_ms)).count();
          const double throughput =
              ByteToMb(benchmark_parameters.object_byte_size) / duration_seconds * benchmark_parameters.thread_count;

          throughputs.emplace_back(throughput);
        }

        const BenchmarkResultAggregate throughput_aggregates(throughputs);

        return std::make_tuple(
            "invocation_throughputs",
            Aws::Utils::Json::JsonValue()
                .WithDouble("invocation_throughput_mb_per_s_minimum", throughput_aggregates.GetMinimum())
                .WithDouble("invocation_throughput_mb_per_s_maximum", throughput_aggregates.GetMaximum())
                .WithDouble("invocation_throughput_mb_per_s_average", throughput_aggregates.GetAverage())
                .WithDouble("invocation_throughput_mb_per_s_median", throughput_aggregates.GetMedian())
                .WithDouble("invocation_throughput_mb_per_s_percentile_0.01", throughput_aggregates.GetPercentile(0.01))
                .WithDouble("invocation_throughput_mb_per_s_percentile_0.1", throughput_aggregates.GetPercentile(0.1))
                .WithDouble("invocation_throughput_mb_per_s_percentile_1", throughput_aggregates.GetPercentile(1))
                .WithDouble("invocation_throughput_mb_per_s_percentile_10", throughput_aggregates.GetPercentile(10))
                .WithDouble("invocation_throughput_mb_per_s_std_dev", throughput_aggregates.GetStandardDeviation()));
      })
      .Build();
}

}  // namespace skyrise
