#pragma once

#include <sstream>

#include <aws/ec2/model/InstanceType.h>

#include "abstract_benchmark_config.hpp"

namespace skyrise {

enum class Ec2InstanceType {
  kC5Large,
  kC5XLarge,
  kC52XLarge,
  kC54XLarge,
  kC59XLarge,
  kC512XLarge,
  kC518XLarge,
  kC524XLarge,
  kC5Metal,
  kT3Nano,
  kT3Micro,
  kT3Small,
  kT3Medium,
  kT3Large,
  kT3XLarge,
  kT32XLarge
};

class Ec2BenchmarkConfig : public AbstractBenchmarkConfig {
 public:
  Ec2BenchmarkConfig(const Ec2InstanceType instance_type, const size_t repetition_count,
                     const size_t concurrent_invocation_count = 1,
                     const std::vector<std::function<void()>>& after_repetition_callbacks = {});

  static Aws::EC2::Model::InstanceType ToAwsType(const Ec2InstanceType instance_type);

  const Aws::EC2::Model::InstanceType instance_type_;
  std::vector<std::string> instance_names_;
};

}  // namespace skyrise
