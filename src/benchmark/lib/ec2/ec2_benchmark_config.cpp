#include "ec2_benchmark_config.hpp"

#include "utils/assert.hpp"

namespace skyrise {

Ec2BenchmarkConfig::Ec2BenchmarkConfig(const Ec2InstanceType instance_type, const size_t repetition_count,
                                       const size_t concurrent_invocation_count,
                                       const std::vector<std::function<void()>>& after_repetition_callbacks)
    : AbstractBenchmarkConfig(concurrent_invocation_count, repetition_count, after_repetition_callbacks),
      instance_type_(ToAwsType(instance_type)) {
  instance_names_.reserve(repetition_count);

  std::stringstream instance_name_base;
  instance_name_base << benchmark_timestamp_ << "-" << benchmark_id_ << "-";

  for (size_t i = 0; i < repetition_count_; i++) {
    instance_names_.push_back(instance_name_base.str() + std::to_string(i));
  }
}

Aws::EC2::Model::InstanceType Ec2BenchmarkConfig::ToAwsType(const Ec2InstanceType instance_type) {
  switch (instance_type) {
    case Ec2InstanceType::kC5Large:
      return Aws::EC2::Model::InstanceType::c5_large;
    case Ec2InstanceType::kC5XLarge:
      return Aws::EC2::Model::InstanceType::c5_xlarge;
    case Ec2InstanceType::kC52XLarge:
      return Aws::EC2::Model::InstanceType::c5_2xlarge;
    case Ec2InstanceType::kC54XLarge:
      return Aws::EC2::Model::InstanceType::c5_4xlarge;
    case Ec2InstanceType::kC59XLarge:
      return Aws::EC2::Model::InstanceType::c5_9xlarge;
    case Ec2InstanceType::kC512XLarge:
      return Aws::EC2::Model::InstanceType::c5_12xlarge;
    case Ec2InstanceType::kC518XLarge:
      return Aws::EC2::Model::InstanceType::c5_18xlarge;
    case Ec2InstanceType::kC524XLarge:
      return Aws::EC2::Model::InstanceType::c5_24xlarge;
    case Ec2InstanceType::kC5Metal:
      return Aws::EC2::Model::InstanceType::c5_metal;
    case Ec2InstanceType::kC5NLarge:
      return Aws::EC2::Model::InstanceType::c5n_large;
    case Ec2InstanceType::kC5NXLarge:
      return Aws::EC2::Model::InstanceType::c5n_xlarge;
    case Ec2InstanceType::kC5N2XLarge:
      return Aws::EC2::Model::InstanceType::c5n_2xlarge;
    case Ec2InstanceType::kC5N4XLarge:
      return Aws::EC2::Model::InstanceType::c5n_4xlarge;
    case Ec2InstanceType::kC5N9XLarge:
      return Aws::EC2::Model::InstanceType::c5n_9xlarge;
    case Ec2InstanceType::kC5N18XLarge:
      return Aws::EC2::Model::InstanceType::c5n_18xlarge;
    case Ec2InstanceType::kC5NMetal:
      return Aws::EC2::Model::InstanceType::c5n_metal;
    case Ec2InstanceType::kD3XLarge:
      return Aws::EC2::Model::InstanceType::d3_xlarge;
    case Ec2InstanceType::kD32XLarge:
      return Aws::EC2::Model::InstanceType::d3_2xlarge;
    case Ec2InstanceType::kD34XLarge:
      return Aws::EC2::Model::InstanceType::d3_4xlarge;
    case Ec2InstanceType::kD38XLarge:
      return Aws::EC2::Model::InstanceType::d3_8xlarge;
    case Ec2InstanceType::kT3Nano:
      return Aws::EC2::Model::InstanceType::t3_nano;
    case Ec2InstanceType::kT3Micro:
      return Aws::EC2::Model::InstanceType::t3_micro;
    case Ec2InstanceType::kT3Small:
      return Aws::EC2::Model::InstanceType::t3_small;
    case Ec2InstanceType::kT3Medium:
      return Aws::EC2::Model::InstanceType::t3_medium;
    case Ec2InstanceType::kT3Large:
      return Aws::EC2::Model::InstanceType::t3_large;
    case Ec2InstanceType::kT3XLarge:
      return Aws::EC2::Model::InstanceType::t3_xlarge;
    case Ec2InstanceType::kT32XLarge:
      return Aws::EC2::Model::InstanceType::t3_2xlarge;
    default:
      Fail("Ec2InstanceType not supported.");
  }
}

}  // namespace skyrise
