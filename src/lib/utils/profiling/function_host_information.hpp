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
  explicit FunctionHostInformationCollector(FunctionHostInformationCollectorConfiguration config);

  FunctionHostInformationIdentification collect_information_identification();
  FunctionHostInformationEnvironment collect_information_environment();
  FunctionHostInformationResources collect_information_resources();
  std::string collect_json();
  std::string as_json(FunctionHostInformationIdentification information_identification,
                      FunctionHostInformationEnvironment information_environment,
                      FunctionHostInformationResources information_resources);

 private:
  struct _CpuInfo {
    size_t cpu_count;
    std::string cpu_model;
    std::string cpu_features;
  };

  FunctionHostInformationCollectorConfiguration _config;

  std::string _id();
  std::string _ip_private();
  std::string _ip_public();

  std::string _operating_system_details();
  std::string _file_system_details();
  size_t _boot_time_seconds();
  size_t _uptime_seconds();

  _CpuInfo _cpu_information();
  size_t _ram_size_mb();

  std::vector<std::string> _find_first(const std::string& regex_string, const std::string& search_string);
  static std::vector<std::vector<std::string>> _find_all(const std::string& regex_string, std::string search_string);
  static std::string _read_file_content(const std::string& filename);
  static std::string _read_stdout(const std::string& command);
};
}  // namespace skyrise
