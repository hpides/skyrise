#pragma once

#include <string>
#include <vector>

#include "micro_benchmark/micro_benchmark_types.hpp"

namespace skyrise {

enum class Ec2InstanceType {
  // Legacy T instances.
  kT22XLarge,
  // T3 instances.
  kT3Nano,
  kT3Micro,
  kT3Small,
  kT3Medium,
  kT3Large,
  kT3XLarge,
  kT32XLarge,
  // T4g instances.
  kT4GNano,
  kT4GMicro,
  kT4GSmall,
  kT4GMedium,
  kT4GLarge,
  kT4GXLarge,
  kT4G2XLarge,
  // Legacy C/M/R instances.
  kM416XLarge,
  kM516XLarge,
  kM5N8XLarge,
  kC5NLarge,
  // M6i instances.
  kM6ILarge,
  kM6IXLarge,
  kM6I2XLarge,
  kM6I4XLarge,
  kM6I8XLarge,
  kM6I12XLarge,
  kM6I16XLarge,
  kM6I24XLarge,
  kM6I32XLarge,
  kM6IMetal,
  // M6g instances.
  kM6GMedium,
  kM6GLarge,
  kM6GXLarge,
  kM6G2XLarge,
  kM6G4XLarge,
  kM6G8XLarge,
  kM6G12XLarge,
  kM6G16XLarge,
  kM6GMetal,
  // C6g instances.
  kC6GMedium,
  kC6GLarge,
  kC6GXLarge,
  kC6G2XLarge,
  kC6G4XLarge,
  kC6G8XLarge,
  kC6G12XLarge,
  kC6G16XLarge,
  // C6gn instances.
  kC6GNMedium,
  kC6GNLarge,
  kC6GNXLarge,
  kC6GN2XLarge,
  kC6GN4XLarge,
  kC6GN8XLarge,
  kC6GN12XLarge,
  kC6GN16XLarge,
  // C7gn instances.
  kC7GN2XLarge
};

size_t Ec2InstanceMemorySizeMiB(Ec2InstanceType instance_type);

enum class Ec2WarmupType { kNone, kDefault };

struct Ec2BenchmarkParameters {
  Ec2InstanceType instance_type;
  size_t concurrent_instance_count;
  size_t repetition_count;
};

struct Ec2StartupBenchmarkParameters {
  Ec2BenchmarkParameters base_parameters;
  Ec2WarmupType warmup_type;
};

struct Ec2StorageBenchmarkParameters {
  Ec2BenchmarkParameters base_parameters;
  StorageBenchmarkParameters storage_parameters;
  std::vector<std::string> sqs_queue_urls;
  std::vector<std::vector<std::string>> repetition_prefixes;
};

}  // namespace skyrise
