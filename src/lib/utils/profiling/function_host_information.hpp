#pragma once

#include <array>
#include <cstddef>
#include <string>
#include <vector>

namespace skyrise {

struct FunctionHostInformationIdentification {
  std::string id;
  std::string ip_private;
  std::string ip_public;
};

struct FunctionHostInformationEnvironment {
  std::string operating_system_details;
  std::string file_system_details;
  size_t boot_time_seconds;
  size_t uptime_seconds;
};

struct FunctionHostInformationResources {
  size_t cpu_count;
  std::string cpu_model;
  std::string cpu_features;
  size_t ram_size_mb;
};

// The default configuration is suitable for the AWS Lambda execution environment
struct FunctionHostInformationCollectorConfiguration {
  std::string cgroup_path = "/proc/self/cgroup";
  std::string ip_private_command = "hostname -I";
  std::string ip_public_command = "curl --silent ipinfo.io/ip";
  bool collect_ip_public = false;

  std::string operating_system_details_command = "uname -sr";
  std::string ls_command = "ls -loR";
  std::string tmp_path = "/tmp";
  std::string stat_path = "/proc/stat";
  std::string uptime_path = "/proc/uptime";

  std::string cpuinfo_path = "/proc/cpuinfo";
  std::string meminfo_path = "/proc/meminfo";

  bool readableJson = false;
};

class FunctionHostInformationCollector {
 public:
  FunctionHostInformationCollector() = default;
  explicit FunctionHostInformationCollector(const FunctionHostInformationCollectorConfiguration& config);

  FunctionHostInformationIdentification CollectInformationIdentification();
  FunctionHostInformationEnvironment CollectInformationEnvironment();
  FunctionHostInformationResources CollectInformationResources();
  std::string CollectJson();
  std::string AsJson(const FunctionHostInformationIdentification& information_identification,
                     const FunctionHostInformationEnvironment& information_environment,
                     const FunctionHostInformationResources& information_resources) const;

 private:
  struct CpuInfo_ {
    size_t cpu_count;
    std::string cpu_model;
    std::string cpu_features;
  };

  FunctionHostInformationCollectorConfiguration config_;

  std::string Id() const;
  std::string IpPrivate() const;
  std::string IpPublic() const;

  std::string OperatingSystemDetails() const;
  std::string FileSystemDetails() const;
  size_t BootTimeSeconds() const;
  size_t UptimeSeconds() const;

  CpuInfo_ CpuInformation() const;
  size_t RamSizeMb() const;

  static std::vector<std::string> FindFirst(const std::string& regex_string, const std::string& search_string);
  static std::vector<std::vector<std::string>> FindAll(const std::string& regex_string, std::string search_string);
  static std::string ReadFileContent(const std::string& filename);
  static std::string ReadStdout(const std::string& command);
};
}  // namespace skyrise
