#include "benchmark_config.hpp"

#include <algorithm>
#include <memory>
#include <string>

#include "limits.hpp"
#include "utils/string.hpp"
#include "utils/time.hpp"

namespace skyrise {

BenchmarkConfig::BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size,
                                 const size_t num_invocations, const ExecuteMode execute_mode)
    : BenchmarkConfig(std::vector<Aws::String>(1, function_zip_name), std::vector<size_t>(1, memory_size),
                      num_invocations, execute_mode, 1, std::vector<std::function<void()>>(),
                      kLambdaFunctionTimeoutSeconds) {}

BenchmarkConfig::BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size,
                                 const size_t num_invocations, const ExecuteMode execute_mode,
                                 const size_t num_repetitions,
                                 const std::vector<std::function<void()>>& after_repetitions_callbacks)
    : BenchmarkConfig(std::vector<Aws::String>(1, function_zip_name), std::vector<size_t>(1, memory_size),
                      num_invocations, execute_mode, num_repetitions, after_repetitions_callbacks,
                      kLambdaFunctionTimeoutSeconds) {}

BenchmarkConfig::BenchmarkConfig(const std::vector<Aws::String>& function_zip_names,
                                 const std::vector<size_t>& memory_sizes, const size_t num_invocations,
                                 const ExecuteMode execute_mode, const size_t num_repetitions,
                                 const std::vector<std::function<void()>>& after_repetition_callbacks,
                                 const size_t timeout)
    : num_invocations_(num_invocations),
      execute_mode_(execute_mode),
      num_repetitions_(num_repetitions),
      after_repetition_callbacks_(after_repetition_callbacks),
      timeout_(timeout),
      benchmark_id_(RandomString(8)),
      benchmark_timestamp_(GetFormattedTimestamp("%Y%m%dT%H%M%S")),
      function_configs_(std::make_shared<std::vector<LambdaFunctionConfig>>()),
      invocation_configs_(std::make_shared<std::vector<LambdaInvocationConfig>>()) {
  const std::shared_ptr<Aws::IOStream> empty_payload = Aws::MakeShared<Aws::StringStream>("");
  if (num_repetitions > 1 && after_repetition_callbacks.size() != num_repetitions) {
    // TODO(anyone): Align with our new error-handling strategy
    throw std::runtime_error("Number of after_repetition_callbacks must be equal to num_repetitions");
  }

  for (size_t function_names_index = 0; function_names_index < function_zip_names.size(); function_names_index++) {
    // TODO(anyone): Make function discovery more flexible and robust
    const Aws::String function_path = "./pkg/" + function_zip_names[function_names_index] + ".zip";

    if (execute_mode == ExecuteMode::kColdAsync || execute_mode == ExecuteMode::kColdParallel ||
        execute_mode == ExecuteMode::kColdSequential) {
      // Add one function config and invocation config per zip and invocation if benchmarking coldstart
      for (size_t invocation_index = 0; invocation_index < num_invocations; invocation_index++) {
        const Aws::String function_name = benchmark_id_ + "-" + benchmark_timestamp_ + "-" +
                                          function_zip_names[function_names_index] + "-" +
                                          std::to_string(function_names_index) + "-" + std::to_string(invocation_index);
        const LambdaFunctionConfig function_config{function_path, function_name, memory_sizes[function_names_index]};
        function_configs_->emplace_back(function_config);

        // If there is one function per invocation, the function name is equal to the invocation ID
        const LambdaInvocationConfig invocation_config{function_name, function_name, empty_payload};
        invocation_configs_->emplace_back(invocation_config);
      }
    } else {
      // Add one function config per zip and one invocation config per invocation if benchmarking warmstart
      const Aws::String function_name =
          benchmark_id_ + "-" + benchmark_timestamp_ + "-" + function_zip_names[function_names_index];
      const LambdaFunctionConfig function_config{function_path, function_name, memory_sizes[function_names_index]};
      function_configs_->emplace_back(function_config);

      for (size_t invocation_index = 0; invocation_index < num_invocations; invocation_index++) {
        const Aws::String invocation_id = benchmark_id_ + "-" + benchmark_timestamp_ + "-" +
                                          function_zip_names[function_names_index] + "-" +
                                          std::to_string(invocation_index);
        const LambdaInvocationConfig invocation_config{function_name, invocation_id, empty_payload};
        invocation_configs_->emplace_back(invocation_config);
      }
    }
  }
}

void BenchmarkConfig::SetPayloads(const std::vector<std::shared_ptr<Aws::IOStream>>& payloads) {
  // Assert payloads size == function_names
  for (size_t payload_index = 0; payload_index < payloads.size(); payload_index++) {
    invocation_configs_->at(payload_index).payload = payloads[payload_index];
  }
}

void BenchmarkConfig::SetOnePayloadForAllFunctions(const std::shared_ptr<Aws::IOStream>& payload) {
  for (auto& config : *invocation_configs_) {
    config.payload = payload;
  }
}

}  // namespace skyrise
