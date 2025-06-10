#include "ec2_benchmark_config.hpp"

#include <magic_enum/magic_enum.hpp>

#include "utils/assert.hpp"

namespace skyrise {

Ec2BenchmarkConfig::Ec2BenchmarkConfig(const Ec2InstanceType instance_type, const size_t concurrent_instance_count,
                                       const size_t repetition_count, const Ec2WarmupType warmup_type,
                                       const std::function<std::string(size_t, size_t)>& user_data,
                                       const std::vector<std::function<void()>>& after_repetition_callbacks,
                                       const std::function<Aws::Utils::Json::JsonValue(size_t)>& benchmark,
                                       const bool enable_vpc)
    : AbstractBenchmarkConfig(concurrent_instance_count, repetition_count, after_repetition_callbacks),
      instance_type_(ToAwsType(instance_type)),
      warmup_type_(warmup_type),
      user_data_(user_data),
      benchmark_(benchmark),
      enable_vpc_(enable_vpc) {
  instance_names_.reserve(repetition_count);

  std::stringstream instance_name_base;
  instance_name_base << benchmark_timestamp_ << "-" << benchmark_id_ << "-";

  for (size_t i = 0; i < repetition_count_; ++i) {
    instance_names_.push_back(instance_name_base.str() + std::to_string(i));
  }
}

std::vector<Ec2InstanceType> Ec2BenchmarkConfig::ParseInstanceTypes(const std::vector<std::string>& instance_types) {
  std::vector<Ec2InstanceType> result_types;
  result_types.reserve(instance_types.size());

  std::ranges::transform(instance_types, std::back_inserter(result_types), [](const std::string& instance_type) {
    const auto parsed_type = magic_enum::enum_cast<skyrise::Ec2InstanceType>(instance_type);
    if (!parsed_type.has_value()) {
      Fail("Instance type '" + instance_type + "' cannot be parsed.");
    }
    return parsed_type.value();
  });

  return result_types;
}

Aws::EC2::Model::InstanceType Ec2BenchmarkConfig::ToAwsType(const Ec2InstanceType instance_type) {
  switch (instance_type) {
    case Ec2InstanceType::kT22XLarge:
      return Aws::EC2::Model::InstanceType::t2_2xlarge;
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
    case Ec2InstanceType::kT4GNano:
      return Aws::EC2::Model::InstanceType::t4g_nano;
    case Ec2InstanceType::kT4GMicro:
      return Aws::EC2::Model::InstanceType::t4g_micro;
    case Ec2InstanceType::kT4GSmall:
      return Aws::EC2::Model::InstanceType::t4g_small;
    case Ec2InstanceType::kT4GMedium:
      return Aws::EC2::Model::InstanceType::t4g_medium;
    case Ec2InstanceType::kT4GLarge:
      return Aws::EC2::Model::InstanceType::t4g_large;
    case Ec2InstanceType::kT4GXLarge:
      return Aws::EC2::Model::InstanceType::t4g_xlarge;
    case Ec2InstanceType::kT4G2XLarge:
      return Aws::EC2::Model::InstanceType::t4g_2xlarge;
    case Ec2InstanceType::kM416XLarge:
      return Aws::EC2::Model::InstanceType::m4_16xlarge;
    case Ec2InstanceType::kM516XLarge:
      return Aws::EC2::Model::InstanceType::m5_16xlarge;
    case Ec2InstanceType::kM5N8XLarge:
      return Aws::EC2::Model::InstanceType::m5n_8xlarge;
    case Ec2InstanceType::kC5NLarge:
      return Aws::EC2::Model::InstanceType::c5n_large;
    case Ec2InstanceType::kM6ILarge:
      return Aws::EC2::Model::InstanceType::m6i_large;
    case Ec2InstanceType::kM6IXLarge:
      return Aws::EC2::Model::InstanceType::m6i_xlarge;
    case Ec2InstanceType::kM6I2XLarge:
      return Aws::EC2::Model::InstanceType::m6i_2xlarge;
    case Ec2InstanceType::kM6I4XLarge:
      return Aws::EC2::Model::InstanceType::m6i_4xlarge;
    case Ec2InstanceType::kM6I8XLarge:
      return Aws::EC2::Model::InstanceType::m6i_8xlarge;
    case Ec2InstanceType::kM6I12XLarge:
      return Aws::EC2::Model::InstanceType::m6i_12xlarge;
    case Ec2InstanceType::kM6I16XLarge:
      return Aws::EC2::Model::InstanceType::m6i_16xlarge;
    case Ec2InstanceType::kM6I24XLarge:
      return Aws::EC2::Model::InstanceType::m6i_24xlarge;
    case Ec2InstanceType::kM6I32XLarge:
      return Aws::EC2::Model::InstanceType::m6i_32xlarge;
    case Ec2InstanceType::kM6IMetal:
      return Aws::EC2::Model::InstanceType::m6i_metal;
    case Ec2InstanceType::kM6GMedium:
      return Aws::EC2::Model::InstanceType::m6g_medium;
    case Ec2InstanceType::kM6GLarge:
      return Aws::EC2::Model::InstanceType::m6g_large;
    case Ec2InstanceType::kM6GXLarge:
      return Aws::EC2::Model::InstanceType::m6g_xlarge;
    case Ec2InstanceType::kM6G2XLarge:
      return Aws::EC2::Model::InstanceType::m6g_2xlarge;
    case Ec2InstanceType::kM6G4XLarge:
      return Aws::EC2::Model::InstanceType::m6g_4xlarge;
    case Ec2InstanceType::kM6G8XLarge:
      return Aws::EC2::Model::InstanceType::m6g_8xlarge;
    case Ec2InstanceType::kM6G12XLarge:
      return Aws::EC2::Model::InstanceType::m6g_12xlarge;
    case Ec2InstanceType::kM6G16XLarge:
      return Aws::EC2::Model::InstanceType::m6g_16xlarge;
    case Ec2InstanceType::kM6GMetal:
      return Aws::EC2::Model::InstanceType::m6g_metal;
    case Ec2InstanceType::kC6GMedium:
      return Aws::EC2::Model::InstanceType::c6g_medium;
    case Ec2InstanceType::kC6GLarge:
      return Aws::EC2::Model::InstanceType::c6g_large;
    case Ec2InstanceType::kC6GXLarge:
      return Aws::EC2::Model::InstanceType::c6g_xlarge;
    case Ec2InstanceType::kC6G2XLarge:
      return Aws::EC2::Model::InstanceType::c6g_2xlarge;
    case Ec2InstanceType::kC6G4XLarge:
      return Aws::EC2::Model::InstanceType::c6g_4xlarge;
    case Ec2InstanceType::kC6G8XLarge:
      return Aws::EC2::Model::InstanceType::c6g_8xlarge;
    case Ec2InstanceType::kC6G12XLarge:
      return Aws::EC2::Model::InstanceType::c6g_12xlarge;
    case Ec2InstanceType::kC6G16XLarge:
      return Aws::EC2::Model::InstanceType::c6g_16xlarge;
    case Ec2InstanceType::kC6GNMedium:
      return Aws::EC2::Model::InstanceType::c6gn_medium;
    case Ec2InstanceType::kC6GNLarge:
      return Aws::EC2::Model::InstanceType::c6gn_large;
    case Ec2InstanceType::kC6GNXLarge:
      return Aws::EC2::Model::InstanceType::c6gn_xlarge;
    case Ec2InstanceType::kC6GN2XLarge:
      return Aws::EC2::Model::InstanceType::c6gn_2xlarge;
    case Ec2InstanceType::kC6GN4XLarge:
      return Aws::EC2::Model::InstanceType::c6gn_4xlarge;
    case Ec2InstanceType::kC6GN8XLarge:
      return Aws::EC2::Model::InstanceType::c6gn_8xlarge;
    case Ec2InstanceType::kC6GN12XLarge:
      return Aws::EC2::Model::InstanceType::c6gn_12xlarge;
    case Ec2InstanceType::kC6GN16XLarge:
      return Aws::EC2::Model::InstanceType::c6gn_16xlarge;
    case Ec2InstanceType::kC7GN2XLarge:
      return Aws::EC2::Model::InstanceType::c7gn_2xlarge;
    default:
      Fail("Ec2InstanceType not supported.");
  }
}

Aws::EC2::Model::InstanceType Ec2BenchmarkConfig::GetInstanceType() const { return instance_type_; }
std::vector<std::string> Ec2BenchmarkConfig::GetInstanceNames() const { return instance_names_; }
Ec2WarmupType Ec2BenchmarkConfig::GetWarmupType() const { return warmup_type_; }
std::function<std::string(size_t, size_t)> Ec2BenchmarkConfig::GetUserData() const { return user_data_; }
std::function<Aws::Utils::Json::JsonValue(size_t)> Ec2BenchmarkConfig::GetBenchmark() const { return benchmark_; }
bool Ec2BenchmarkConfig::IsVpcEnabled() const { return enable_vpc_; }

}  // namespace skyrise
