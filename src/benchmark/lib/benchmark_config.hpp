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

/*
 * Cold-*: Create individual function per invocation to measure coldstart latency
 * Warm-*: Warm up functions by running them before measurement
 * *-Sequential: RequestResponse-Functions, invoked sequentially
 * *-Parallel: RequestResponse-Functions, invoked in parallel
 * *-Async: Event-Function, invoked in parallel
 */
enum class ExecuteMode { ColdSequential, ColdParallel, ColdAsync, WarmSequential, WarmParallel, WarmAsync };

class BenchmarkConfig {
 public:
  BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size, const Aws::String& function_role,
                  const size_t num_invocations, const ExecuteMode execute_mode);
  BenchmarkConfig(const Aws::String& function_zip_name, const size_t memory_size, const Aws::String& function_role,
                  const size_t num_invocations, const ExecuteMode execute_mode, const size_t num_repetitions,
                  const std::vector<std::function<void()>>& after_repetitions_callbacks);
  BenchmarkConfig(const std::vector<Aws::String>& function_zip_names, const std::vector<size_t>& memory_sizes,
                  const Aws::String& function_role, const size_t num_invocations, const ExecuteMode execute_mode,
                  const size_t num_repetitions, const std::vector<std::function<void()>>& after_repetition_callbacks,
                  const size_t timeout);

  void SetPayloads(const std::vector<std::shared_ptr<Aws::IOStream>>& payloads);
  void SetOnePayloadForAllFunctions(const std::shared_ptr<Aws::IOStream>& payload);

  const Aws::String function_role_name_;
  const size_t num_invocations_;
  const ExecuteMode execute_mode_;
  const size_t num_repetitions_;
  const std::vector<std::function<void()>> after_repetition_callbacks_;
  const size_t timeout_;

  const Aws::String benchmark_id_;
  const Aws::String benchmark_timestamp_;
  const std::shared_ptr<std::vector<LambdaFunctionConfig>> function_configs_;
  const std::shared_ptr<std::vector<LambdaInvocationConfig>> invocation_configs_;

 private:
  static Aws::String GetRandomString();
  static Aws::String GetTimestamp();
};

}  // namespace skyrise
