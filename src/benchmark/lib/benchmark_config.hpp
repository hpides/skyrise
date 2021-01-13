#pragma once

#include <chrono>

#include <aws/core/Aws.h>

namespace skyrise {

struct LambdaFunctionConfig {
  Aws::String function_path;
  Aws::String function_name;
  size_t memory_size;
};

struct LambdaInvocationConfig {
  Aws::String function_name;
  Aws::String invocation_id;
  std::shared_ptr<Aws::IOStream> payload;
};

enum class WarmUpStrategy { kNone, kDefault };

enum class UseOneFunctionPerRepetition : bool { kYes = true, kNo = false };

enum class UseEventQueue : bool { kYes = true, kNo = false };

class BenchmarkConfig {
 public:
  BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size, const size_t repetition_count,
                  const size_t concurrent_invocation_count = 1,
                  const WarmUpStrategy warm_up_strategy = WarmUpStrategy::kNone,
                  const UseOneFunctionPerRepetition use_one_function_per_repetition = UseOneFunctionPerRepetition::kNo,
                  const UseEventQueue use_event_queue = UseEventQueue::kNo,
                  const std::vector<std::function<void()>>& after_repetition_callbacks = {});

  void SetPayloads(const std::vector<std::shared_ptr<Aws::IOStream>>& payloads);
  void SetOnePayloadForAllFunctions(const std::shared_ptr<Aws::IOStream>& payload);

  const size_t repetition_count_;
  const size_t concurrent_invocation_count_;
  const WarmUpStrategy warm_up_strategy_;
  const UseOneFunctionPerRepetition use_one_function_per_repetition_;
  const UseEventQueue use_event_queue_;
  const std::vector<std::function<void()>> after_repetition_callbacks_;

  const Aws::String benchmark_id_;
  const Aws::String benchmark_timestamp_;
  std::vector<LambdaFunctionConfig> function_configs_;
  std::vector<std::vector<LambdaInvocationConfig>> repetition_configs_;
};

}  // namespace skyrise
