#include "benchmark_config.hpp"

#include <algorithm>
#include <memory>
#include <string>

#include "limits.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"
#include "utils/time.hpp"

namespace skyrise {

BenchmarkConfig::BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size,
                                 const size_t repetition_count, const size_t concurrent_invocation_count,
                                 const WarmUpStrategy warm_up_strategy,
                                 const UseOneFunctionPerRepetition use_one_function_per_repetition,
                                 const UseEventQueue use_event_queue,
                                 const std::vector<std::function<void()>>& after_repetition_callbacks)
    : repetition_count_(repetition_count),
      concurrent_invocation_count_(concurrent_invocation_count),
      warm_up_strategy_(warm_up_strategy),
      use_one_function_per_repetition_(use_one_function_per_repetition),
      use_event_queue_(use_event_queue),
      after_repetition_callbacks_(after_repetition_callbacks.empty()
                                      ? std::vector<std::function<void()>>(repetition_count_, [] {})
                                      : after_repetition_callbacks),
      benchmark_id_(RandomString(8)),
      benchmark_timestamp_(GetFormattedTimestamp("%Y%m%dT%H%M%S")) {
  Assert(after_repetition_callbacks_.size() == repetition_count_,
         "The number of repetition callbacks and the repetition count must be equal.");

  // TODO(anyone): Make function discovery more flexible and robust
  const Aws::String function_path = "./pkg/" + function_zip_name + ".zip";
  Aws::StringStream function_name_base;
  function_name_base << benchmark_id_ << "-" << benchmark_timestamp_ << "-" << function_zip_name;

  if (use_one_function_per_repetition_ == UseOneFunctionPerRepetition::kYes) {
    for (size_t i = 0; i < repetition_count_; i++) {
      function_configs_.emplace_back(
          LambdaFunctionConfig{function_path, function_name_base.str() + "-" + std::to_string(i), memory_size});
    }
  } else {
    function_configs_.emplace_back(LambdaFunctionConfig{function_path, function_name_base.str(), memory_size});
  }

  auto empty_payload = std::make_shared<Aws::StringStream>();

  for (size_t i = 0; i < repetition_count_; i++) {
    std::vector<LambdaInvocationConfig> invocation_configs;
    invocation_configs.reserve(concurrent_invocation_count_);

    const Aws::String function_name = use_one_function_per_repetition_ == UseOneFunctionPerRepetition::kYes
                                          ? function_configs_[i].function_name
                                          : function_name_base.str();

    for (size_t j = 0; j < concurrent_invocation_count_; j++) {
      invocation_configs.emplace_back(LambdaInvocationConfig{
          function_name, function_name_base.str() + "-" + std::to_string(i) + "-" + std::to_string(j), empty_payload});
    }

    repetition_configs_.emplace_back(invocation_configs);
  }
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
