#include "benchmark_config.hpp"

#include <algorithm>
#include <climits>
#include <memory>
#include <string>

#include <unistd.h>

#include "utils/assert.hpp"
#include "utils/string.hpp"
#include "utils/time.hpp"

namespace skyrise {

BenchmarkConfig::BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size,
                                 const size_t repetition_count, const size_t concurrent_invocation_count,
                                 const WarmUp warm_up,
                                 const UseOneFunctionPerRepetition use_one_function_per_repetition,
                                 const UseEventQueue use_event_queue,
                                 const std::vector<std::function<void()>>& after_repetition_callbacks,
                                 const Aws::String& function_bucket, const bool enable_tracing)
    : repetition_count_(repetition_count),
      concurrent_invocation_count_(concurrent_invocation_count),
      warm_up_(warm_up),
      use_one_function_per_repetition_(use_one_function_per_repetition),
      use_event_queue_(use_event_queue),
      after_repetition_callbacks_(after_repetition_callbacks.empty()
                                      ? std::vector<std::function<void()>>(repetition_count_, [] {})
                                      : after_repetition_callbacks),
      enable_tracing_(enable_tracing),
      benchmark_id_(RandomString(8)),
      benchmark_timestamp_(GetFormattedTimestamp("%Y%m%dT%H%M%S")) {
  Assert(after_repetition_callbacks_.size() == repetition_count_,
         "The number of repetition callbacks and the repetition count must be equal.");

  switch (warm_up_) {
    case WarmUp::kNone: {
      warm_up_strategy_ = nullptr;
      break;
    }
    case WarmUp::kDefault: {
      warm_up_strategy_ = std::make_shared<SleepWarmUpStrategy>(true);
      break;
    }
    case WarmUp::kDefaultOncePerRepetition: {
      warm_up_strategy_ = std::make_shared<SleepWarmUpStrategy>(false);
      break;
    }
    default:
      warm_up_strategy_ = nullptr;
      break;
  }

  const Aws::String function_path = GetProjectDirPath() + "pkg/" + function_zip_name + ".zip";
  const auto is_local = function_zip_name.find("S3_") != 0;
  const Aws::String function_location =
      is_local ? GetProjectDirPath() + "pkg/" + function_zip_name + ".zip" : function_bucket;
  Aws::StringStream function_name_base;
  function_name_base << benchmark_id_ << "-" << benchmark_timestamp_ << "-" << function_zip_name;

  if (use_one_function_per_repetition_ == UseOneFunctionPerRepetition::kYes) {
    for (size_t i = 0; i < repetition_count_; i++) {
      function_configs_.emplace_back(
          FunctionConfig{function_location, function_name_base.str() + "-" + std::to_string(i), memory_size, is_local});
    }
  } else {
    function_configs_.emplace_back(FunctionConfig{function_location, function_name_base.str(), memory_size, is_local});
  }

  auto empty_payload = std::make_shared<Aws::StringStream>();

  for (size_t i = 0; i < repetition_count_; i++) {
    std::vector<FunctionInvocationConfig> invocation_configs;
    invocation_configs.reserve(concurrent_invocation_count_);

    const Aws::String function_name = use_one_function_per_repetition_ == UseOneFunctionPerRepetition::kYes
                                          ? function_configs_[i].function_name
                                          : function_name_base.str();

    for (size_t j = 0; j < concurrent_invocation_count_; j++) {
      invocation_configs.emplace_back(FunctionInvocationConfig{
          function_name, function_name_base.str() + "-" + std::to_string(i) + "-" + std::to_string(j), empty_payload});
    }

    repetition_configs_.emplace_back(invocation_configs);
  }
}

Aws::String BenchmarkConfig::GetProjectDirPath() {
  std::array<char, PATH_MAX> executable_path_buffer{};
  const auto path_name_length =
      readlink("/proc/self/exe", executable_path_buffer.data(), sizeof(executable_path_buffer) - 1);

  if (path_name_length == -1) {
    Fail("Unable to read project directory path.");
  }

  executable_path_buffer[path_name_length] = '\0';

  const Aws::String path_name(executable_path_buffer.data());

  // Return the absolute project directory path by removing the path to the executable
  return path_name.substr(0, path_name.rfind("bin"));
}

void BenchmarkConfig::SetPayloads(const std::vector<std::shared_ptr<Aws::IOStream>>& payloads) {
  Assert(payloads.size() == concurrent_invocation_count_,
         "The number of payloads and the concurrent invocation count must be equal.");

  for (size_t i = 0; i < repetition_count_; i++) {
    for (size_t j = 0; j < concurrent_invocation_count_; j++) {
      repetition_configs_[i][j].payload = payloads[j];
    }
  }
}

void BenchmarkConfig::SetOnePayloadForAllFunctions(const std::shared_ptr<Aws::IOStream>& payload) {
  for (auto& repetition_config : repetition_configs_) {
    for (auto& invocation_config : repetition_config) {
      invocation_config.payload = payload;
    }
  }
}

}  // namespace skyrise
