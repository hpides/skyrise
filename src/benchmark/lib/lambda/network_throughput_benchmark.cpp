#include "network_throughput_benchmark.hpp"

#include <algorithm>
#include <numeric>
#include <tuple>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace {

const Aws::String kName = "network_throughput_benchmark";

}  // namespace

namespace skyrise {

NetworkThroughputBenchmark::NetworkThroughputBenchmark(
    std::shared_ptr<const BenchmarkHelper> helper, std::shared_ptr<const CostCalculator> cost_calculator,
    const std::vector<size_t>& function_instance_mb_sizes, const std::vector<size_t>& object_byte_sizes,
    const std::vector<size_t>& batch_sizes, const std::vector<size_t>& thread_counts, const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator)) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto object_byte_size : object_byte_sizes) {
      for (const auto batch_size : batch_sizes) {
        for (const auto thread_count : thread_counts) {
          // Reject read/write configurations where the total object size to process is more than half of the function
          // instance size to prevent failure
          if (thread_count * object_byte_size <= MbToByte(function_instance_mb_size) / 2) {
            for (const auto operation_type : {S3OperationType::kWrite, S3OperationType::kRead}) {
              Aws::StringStream function_name;
              function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write")
                            << "S3";

              const auto config = std::make_shared<LambdaBenchmarkConfig>(function_name.str(),
                                                                          function_instance_mb_size, repetition_count);
              const NetworkBenchmarkParameters parameters{
                  function_instance_mb_size, object_byte_size, batch_size, thread_count, 1, 1,
                  repetition_count,          operation_type};
              config->SetPayloads(GeneratePayloads(parameters));

              benchmark_configs_.emplace_back(parameters, config);
            }
          }
        }
      }
    }
  }
}

Aws::Utils::Json::JsonValue NetworkThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const NetworkBenchmarkParameters& benchmark_parameters) {
  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> throughputs;
  throughputs.reserve(benchmark_repetitions.size() * benchmark_repetitions.front().GetInvokeResults().size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    for (const auto& invoke_result : benchmark_repetition.GetInvokeResults()) {
      if (invoke_result.IsSuccess()) {
        const auto ms_durations = invoke_result.GetResponseBody().GetArray("ms_durations");

        for (size_t i = 0; i < ms_durations.GetLength(); ++i) {
          const double seconds_duration =
              std::chrono::duration<double>(std::chrono::duration<double, std::milli>(ms_durations[i].AsDouble()))
                  .count();
          throughputs.emplace_back(ByteToMb(benchmark_parameters.object_byte_size) * benchmark_parameters.thread_count /
                                   seconds_duration);
        }
      }
    }
  }

  if (throughputs.empty()) {
    throughputs.emplace_back(-1.0);
  }

  const BenchmarkResultAggregate aggregate(throughputs);

  LambdaBenchmarkOutput output(Name(), benchmark_result);

  return AddArguments(output, benchmark_parameters)
      .WithDoubleMetric("benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                                  benchmark_result, benchmark_parameters.function_instance_mb_size)))
      .WithDoubleMetric("throughput_mb_per_s_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("throughput_mb_per_s_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("throughput_mb_per_s_average", aggregate.GetAverage())
      .WithDoubleMetric("throughput_mb_per_s_median", aggregate.GetMedian())
      .WithDoubleMetric("throughput_mb_per_s_percentile_0.01", aggregate.GetPercentile(0.01))
      .WithDoubleMetric("throughput_mb_per_s_percentile_0.1", aggregate.GetPercentile(0.1))
      .WithDoubleMetric("throughput_mb_per_s_percentile_1", aggregate.GetPercentile(1))
      .WithDoubleMetric("throughput_mb_per_s_percentile_10", aggregate.GetPercentile(10))
      .WithDoubleMetric("throughput_mb_per_s_std_dev", aggregate.GetStandardDeviation())
      .WithDoubleInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("function_cost_usd",
                               ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size));
      })
      .WithDoubleInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("invocation_billed_duration_ms", invoke_result.GetLogResult()->GetBilledDurationMs());
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
      .Build();
}

const Aws::String& NetworkThroughputBenchmark::Name() const { return kName; }

}  // namespace skyrise
