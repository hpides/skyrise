#include "benchmark_config.hpp"

#include <algorithm>
#include <ctime>
#include <iomanip>
#include <memory>
#include <random>
#include <string>

namespace skyrise {

BenchmarkConfig::BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size,
                                 const Aws::String& function_role, const size_t num_invocations,
                                 const ExecuteMode execute_mode)
    : BenchmarkConfig(std::vector<Aws::String>(1, function_zip_name), std::vector<size_t>(1, memory_size),
                      function_role, num_invocations, execute_mode, 900) {}

BenchmarkConfig::BenchmarkConfig(const std::vector<Aws::String>& function_zip_names,
                                 const std::vector<size_t>& memory_sizes, const Aws::String& function_role,
                                 const size_t num_invocations, const ExecuteMode execute_mode, const size_t timeout)
    : function_role_name_(function_role),
      num_invocations_(num_invocations),
      execute_mode_(execute_mode),
      timeout_(timeout),
      benchmark_id_(GetRandomString()),
      benchmark_timestamp_(GetTimestamp()),
      function_configs_(std::make_shared<std::vector<LambdaFunctionConfig>>()),
      invocation_configs_(std::make_shared<std::vector<LambdaInvocationConfig>>()) {
  const std::shared_ptr<Aws::IOStream> empty_payload = Aws::MakeShared<Aws::StringStream>("");

  for (size_t function_names_index = 0; function_names_index < function_zip_names.size(); function_names_index++) {
    // TODO: Make function discovery more flexible and robust
    const Aws::String function_path = "./pkg/" + function_zip_names[function_names_index] + ".zip";

    if (execute_mode == ExecuteMode::ColdAsync || execute_mode == ExecuteMode::ColdParallel ||
        execute_mode == ExecuteMode::ColdSequential) {
      // Add one function config and invocation config per zip and invocation if benchmarking coldstart
      for (size_t invocation_index = 0; invocation_index < num_invocations; invocation_index++) {
        const Aws::String function_name = benchmark_id_ + "-" + benchmark_timestamp_ + "-" +
                                          function_zip_names[function_names_index] + "-" +
                                          std::to_string(invocation_index);
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

void BenchmarkConfig::SetPayloads(std::vector<std::shared_ptr<Aws::IOStream>>& payloads) {
  // Assert payloads size == function_names
  for (size_t payload_index = 0; payload_index < payloads.size(); payload_index++) {
    invocation_configs_->at(payload_index).payload = payloads[payload_index];
  }
}

void BenchmarkConfig::SetOnePayloadForAllFunctions(std::shared_ptr<Aws::IOStream> payload) {
  for (auto& config : *invocation_configs_) {
    config.payload = payload;
  }
}

Aws::String BenchmarkConfig::GetRandomString() {
  const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  const size_t max_index = (sizeof(charset) - 1);

  std::default_random_engine random_number_generator(std::random_device{}());
  std::uniform_int_distribution<> distribution(0, max_index);

  std::string random_string(8, 0);
  std::generate_n(random_string.begin(), 8, [&]() { return charset[distribution(random_number_generator)]; });

  return random_string;
}

Aws::String BenchmarkConfig::GetTimestamp() {
  const auto time = std::time(nullptr);
  const auto localtime = *std::localtime(&time);
  std::stringstream timestamp;
  timestamp << std::put_time(&localtime, "%Y%m%dT%H%M%S");

  return timestamp.str();
}

}  // namespace skyrise
