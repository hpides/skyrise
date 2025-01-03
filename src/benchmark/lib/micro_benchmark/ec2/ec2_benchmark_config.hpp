#pragma once

#include <sstream>

#include <aws/ec2/model/InstanceType.h>

#include "abstract_benchmark_config.hpp"
#include "configuration.hpp"
#include "ec2_benchmark_result.hpp"
#include "ec2_benchmark_types.hpp"

namespace skyrise {

const std::string kHashbang = "#!/bin/bash\n";
const std::string kDefaultUserData =
    "echo 'echo \"shutdown -h\" | at now + " + std::to_string(kEc2TerminationGuardMinutes) +
    " minutes' > /var/lib/cloud/scripts/per-boot/shutdown.sh && chmod +x "
    "/var/lib/cloud/scripts/per-boot/shutdown.sh && /var/lib/cloud/scripts/per-boot/shutdown.sh";

class Ec2BenchmarkConfig : public AbstractBenchmarkConfig {
 public:
  Ec2BenchmarkConfig(
      const Ec2InstanceType instance_type, const size_t concurrent_instance_count, const size_t repetition_count,
      const Ec2WarmupType warmup_type = Ec2WarmupType::kNone,
      const std::function<std::string(size_t, size_t)>& user_data = std::function<std::string(size_t, size_t)>(
          [](size_t /*repetition_id*/, size_t /*invocation_id*/) { return kHashbang + kDefaultUserData; }),
      const std::vector<std::function<void()>>& after_repetition_callbacks = {},
      const std::function<Aws::Utils::Json::JsonValue(size_t)>& benchmark =
          std::function<Aws::Utils::Json::JsonValue(size_t)>([](size_t /*repetition_id*/) {
            return Aws::Utils::Json::JsonValue();
          }),
      const bool enable_vpc = false);

  static std::vector<Ec2InstanceType> ParseInstanceTypes(const std::vector<std::string>& instance_types);
  static Aws::EC2::Model::InstanceType ToAwsType(const Ec2InstanceType instance_type);

  Aws::EC2::Model::InstanceType GetInstanceType() const;
  std::vector<std::string> GetInstanceNames() const;
  Ec2WarmupType GetWarmupType() const;
  std::function<std::string(size_t, size_t)> GetUserData() const;
  std::function<Aws::Utils::Json::JsonValue(size_t)> GetBenchmark() const;
  bool IsVpcEnabled() const;

 protected:
  const Aws::EC2::Model::InstanceType instance_type_;
  std::vector<std::string> instance_names_;
  const Ec2WarmupType warmup_type_;
  const std::function<std::string(size_t, size_t)> user_data_;
  std::function<Aws::Utils::Json::JsonValue(size_t)> benchmark_;
  const bool enable_vpc_;
};

}  // namespace skyrise
