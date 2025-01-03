#pragma once

#include "micro_benchmark/micro_benchmark_types.hpp"

namespace skyrise {

enum class DeploymentType { kS3Object, kLocalZipFile };

enum class EventQueue { kNo, kYes };

enum class DistinctFunctionPerRepetition { kNo, kYes };

enum class Warmup { kNo, kYesOnInitialRepetition, kYesOnEveryRepetition };

struct LambdaBenchmarkParameters {
  size_t function_instance_size_mb{0};
  size_t concurrent_instance_count{0};
  size_t repetition_count{0};
  size_t after_repetition_delay_min{0};
};

struct LambdaStartupBenchmarkParameters {
  LambdaBenchmarkParameters base_parameters;
  Aws::String package_name;
  size_t payload_size_bytes{0};
  EventQueue is_event{EventQueue::kNo};
  Warmup is_warm{Warmup::kNo};
};

struct LambdaStorageBenchmarkParameters {
  LambdaBenchmarkParameters base_parameters;
  StorageBenchmarkParameters storage_parameters;
};

struct LambdaWarmupBenchmarkParameters {
  LambdaBenchmarkParameters base_parameters;
  std::string warmup_strategy;
  bool is_provisioned{false};
  double provisioning_factor{0};
  size_t warmup_interval_min{0};
};

}  // namespace skyrise
