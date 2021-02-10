#pragma once

#include <chrono>

#include <aws/core/Aws.h>

#include "function_config.hpp"
#include "function_invocation_config.hpp"
#include "warm_up_strategy.hpp"

namespace skyrise {

enum class WarmUp { kNone, kDefault, kDefaultOncePerRepetition };

enum class UseOneFunctionPerRepetition : bool { kYes = true, kNo = false };

enum class UseEventQueue : bool { kYes = true, kNo = false };

class BenchmarkConfig {
 public:
  BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size, const size_t repetition_count,
                  const size_t concurrent_invocation_count = 1, const WarmUp warm_up = WarmUp::kNone,
                  const UseOneFunctionPerRepetition use_one_function_per_repetition = UseOneFunctionPerRepetition::kNo,
                  const UseEventQueue use_event_queue = UseEventQueue::kNo,
                  const std::vector<std::function<void()>>& after_repetition_callbacks = {},
                  const Aws::String& function_bucket = "", const bool enable_tracing = false);

  void SetPayloads(const std::vector<std::shared_ptr<Aws::IOStream>>& payloads);
  void SetOnePayloadForAllFunctions(const std::shared_ptr<Aws::IOStream>& payload);
  static Aws::String GetProjectDirPath();

  const size_t repetition_count_;
  const size_t concurrent_invocation_count_;
  const WarmUp warm_up_;
  const UseOneFunctionPerRepetition use_one_function_per_repetition_;
  const UseEventQueue use_event_queue_;
  const std::vector<std::function<void()>> after_repetition_callbacks_;
  const bool enable_tracing_;

  const Aws::String benchmark_id_;
  const Aws::String benchmark_timestamp_;
  std::shared_ptr<WarmUpStrategy> warm_up_strategy_;
  std::vector<FunctionConfig> function_configs_;
  std::vector<std::vector<FunctionInvocationConfig>> repetition_configs_;

 private:
};

}  // namespace skyrise
