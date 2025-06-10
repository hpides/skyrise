#pragma once

#include <chrono>

#include <aws/core/Aws.h>

#include "abstract_benchmark_config.hpp"
#include "function/function_config.hpp"
#include "lambda_benchmark_types.hpp"
#include "lambda_invocation_config.hpp"
#include "scheduler/warmup_strategy.hpp"

namespace skyrise {

class LambdaBenchmarkConfig : public AbstractBenchmarkConfig {
  friend class LambdaBenchmarkRunner;

 public:
  LambdaBenchmarkConfig(
      const Aws::String& function_package_name, const Aws::String& s3_bucket_name, const Warmup warmup = Warmup::kNo,
      const DistinctFunctionPerRepetition distinct_function_per_repetition = DistinctFunctionPerRepetition::kNo,
      const EventQueue use_event_queue = EventQueue::kNo, const bool enable_tracing = false,
      const size_t function_memory_size_mb = 128, const std::string& efs_arn = "",
      const size_t concurrent_instance_count = 1, const size_t repetition_count = 1,
      const std::vector<std::function<void()>>& after_repetition_callbacks = {}, const bool enable_vpc = false);

  Warmup GetWarmupType() const;
  EventQueue UseEventQueue() const;
  std::shared_ptr<WarmupStrategy> GetWarmupStrategy() const;
  void SetPayloads(const std::vector<std::shared_ptr<Aws::IOStream>>& payloads);
  void SetOnePayloadForAllFunctions(const std::shared_ptr<Aws::IOStream>& payload);
  void SetWarmupStrategy(std::shared_ptr<WarmupStrategy> warmup_strategy);

 protected:
  const Warmup warmup_type_;
  const DistinctFunctionPerRepetition distinct_function_per_repetition_;
  const enum EventQueue use_event_queue_;
  // TODO(tobodner): Investigate if we still use enable_tracing_.
  const bool enable_tracing_;

  std::vector<FunctionConfig> function_configs_;
  std::vector<std::vector<LambdaInvocationConfig>> repetition_configs_;
  std::shared_ptr<WarmupStrategy> warmup_strategy_;
};

}  // namespace skyrise
