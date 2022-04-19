#include "ec2/ec2_benchmark_config.hpp"

#include <gtest/gtest.h>

namespace skyrise {

const std::vector<Ec2InstanceType> kInstanceTypes{
    Ec2InstanceType::kC5Large,    Ec2InstanceType::kC5XLarge,   Ec2InstanceType::kC52XLarge,
    Ec2InstanceType::kC54XLarge,  Ec2InstanceType::kC59XLarge,  Ec2InstanceType::kC512XLarge,
    Ec2InstanceType::kC518XLarge, Ec2InstanceType::kC524XLarge, Ec2InstanceType::kC5Metal,
    Ec2InstanceType::kT3Nano,     Ec2InstanceType::kT3Micro,    Ec2InstanceType::kT3Small,
    Ec2InstanceType::kT3Medium,   Ec2InstanceType::kT3Large,    Ec2InstanceType::kT3XLarge,
    Ec2InstanceType::kT32XLarge};

TEST(EC2BenchmarkConfigTest, EC2InstanceMapping) {
  for (const auto instance_type : kInstanceTypes) {
    Ec2BenchmarkConfig config(instance_type, 2, 3);
    EXPECT_EQ(config.instance_names_.size(), 2);
    EXPECT_EQ(config.concurrent_invocation_count_, 3);
    EXPECT_EQ(config.instance_type_, Ec2BenchmarkConfig::ToAwsType(instance_type));
  }
}

}  // namespace skyrise
