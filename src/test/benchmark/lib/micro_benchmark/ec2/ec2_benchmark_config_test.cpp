#include "micro_benchmark/ec2/ec2_benchmark_config.hpp"

#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>

namespace skyrise {

TEST(Ec2BenchmarkConfigTest, InstanceParsing) {
  //  [ Ec2InstanceType::kT3Nano, Ec2InstanceType::kT4GNano, ... ]
  auto all_instance_types = magic_enum::enum_values<Ec2InstanceType>();
  std::vector<Ec2InstanceType> expected_instance_types(all_instance_types.begin(), all_instance_types.end());

  // [ "kT3Nano", "kT4GNano", ... ]
  auto all_instance_names = magic_enum::enum_names<Ec2InstanceType>();
  std::vector<std::string> instance_types(all_instance_names.begin(), all_instance_names.end());

  std::vector<Ec2InstanceType> parsed_instance_types = Ec2BenchmarkConfig::ParseInstanceTypes(instance_types);
  EXPECT_EQ(parsed_instance_types, expected_instance_types);
  EXPECT_ANY_THROW(Ec2BenchmarkConfig::ParseInstanceTypes({"T3Nano"}));
}

TEST(Ec2BenchmarkConfigTest, InstanceMapping) {
  for (const auto instance_type : magic_enum::enum_values<Ec2InstanceType>()) {
    std::cout << magic_enum::enum_name(instance_type) << std::endl;
    const Ec2BenchmarkConfig config(instance_type, 3, 2);
    EXPECT_EQ(config.GetInstanceType(), Ec2BenchmarkConfig::ToAwsType(instance_type));
    EXPECT_EQ(config.GetInstanceNames().size(), 2);
    EXPECT_EQ(config.GetConcurrentInstanceCount(), 3);
  }
}

}  // namespace skyrise
