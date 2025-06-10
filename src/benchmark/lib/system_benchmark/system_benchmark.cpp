#include "system_benchmark.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "systemBenchmark";

}  // namespace

SystemBenchmark::SystemBenchmark(std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
                                 std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
                                 std::shared_ptr<const Aws::S3::S3Client> s3_client, const CompilerName& compiler_name,
                                 const QueryId& query_id, const ScaleFactor& scale_factor,
                                 const size_t concurrent_instance_count, const size_t repetition_count,
                                 const std::vector<size_t>& after_repetition_delays_min)
    : iam_client_(std::move(iam_client)), lambda_client_(std::move(lambda_client)), s3_client_(std::move(s3_client)) {
  for (const auto after_repetition_delay_min : after_repetition_delays_min) {
    std::vector<std::function<void()>> after_repetition_callbacks;
    after_repetition_callbacks.reserve(repetition_count);

    for (size_t i = 0; i < repetition_count - 1; ++i) {
      after_repetition_callbacks.emplace_back([after_repetition_delay_min]() {
        std::this_thread::sleep_for(std::chrono::minutes(after_repetition_delay_min));
      });
    }

    after_repetition_callbacks.emplace_back([]() {});

    config_ = std::make_shared<SystemBenchmarkConfig>(compiler_name, query_id, scale_factor, concurrent_instance_count,
                                                      repetition_count, after_repetition_callbacks);
  }
}

const Aws::String& SystemBenchmark::Name() const { return kName; }

Aws::Utils::Array<Aws::Utils::Json::JsonValue> SystemBenchmark::Run(
    const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) {
  const auto system_benchmark_runner = std::dynamic_pointer_cast<SystemBenchmarkRunner>(benchmark_runner);
  Assert(system_benchmark_runner, "SystemBenchmark needs a SystemBenchmarkRunner to run.");
  return OnRun(system_benchmark_runner);
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> SystemBenchmark::OnRun(
    const std::shared_ptr<SystemBenchmarkRunner>& runner) {
  return runner->RunSystemConfig(config_)->GetResults();
}

}  // namespace skyrise
