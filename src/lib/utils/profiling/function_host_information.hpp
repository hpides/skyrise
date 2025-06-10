#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace skyrise {

class NetworkInterface {
 public:
  NetworkInterface(std::shared_ptr<ifaddrs> interface_address_list_head, ifaddrs* current_interface_address)
      : interface_address_list_head_(std::move(interface_address_list_head)),
        current_interface_address_(current_interface_address) {}
  // NOLINTNEXTLINE(google-explicit-constructor,hicpp-explicit-conversions)
  NetworkInterface(const std::shared_ptr<ifaddrs>& interface_address_list_head)
      : NetworkInterface(interface_address_list_head, interface_address_list_head.get()) {}

  static NetworkInterface GetFirstInterface();
  NetworkInterface Next();
  bool IsValid() { return current_interface_address_ != nullptr; }
  bool HasAddress() { return current_interface_address_->ifa_addr != nullptr; }
  bool IsUp() { return current_interface_address_->ifa_flags & IFF_UP; }
  bool IsLoopback() { return current_interface_address_->ifa_flags & IFF_LOOPBACK; }
  bool IsIpv4() { return current_interface_address_->ifa_addr->sa_family == AF_INET; }
  bool IsIpv6() { return current_interface_address_->ifa_addr->sa_family == AF_INET6; }

  bool IsIpv6LinkLocalAddress();
  std::string GetNumericHostname();

 private:
  std::shared_ptr<ifaddrs> interface_address_list_head_;
  // This is a raw pointer, pointing at some item in the linked list started with `interface_address_list_head_`.
  // It shares the lifetime of the smart pointer above.
  ifaddrs* current_interface_address_;
};

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

struct FunctionHostInformationResourceUsage {
  size_t process_used_memory_in_ram_kb;
  size_t process_used_memory_kb;
  size_t system_available_memory_kb;
  size_t system_free_memory_kb;
};
struct SystemMemoryUsage {
  size_t available_memory_kb;
  size_t free_memory_kb;
};

struct ProcessMemoryUsage {
  size_t physical_memory_kb;
  size_t virtual_memory_kb;
};

// The default configuration is suitable for the AWS Lambda execution environment
struct FunctionHostInformationCollectorConfiguration {
  std::string cgroup_path = "/proc/self/cgroup";
  std::string ip_public_command = "curl --silent ipinfo.io/ip";
  bool collect_ip_public = false;

  std::string operating_system_details_command = "uname -sr";
  std::string ls_command = "ls -loR";
  std::string tmp_path = "/tmp";
  std::string stat_path = "/proc/stat";
  std::string uptime_path = "/proc/uptime";

  std::string cpuinfo_path = "/proc/cpuinfo";
  std::string meminfo_path = "/proc/meminfo";
  std::string self_status_path = "/proc/self/status";

  bool readable_json = false;
};

class FunctionHostInformationCollector {
 public:
  FunctionHostInformationCollector() = default;
  explicit FunctionHostInformationCollector(const FunctionHostInformationCollectorConfiguration& config);

  FunctionHostInformationIdentification CollectInformationIdentification();
  FunctionHostInformationEnvironment CollectInformationEnvironment();
  FunctionHostInformationResources CollectInformationResources();
  FunctionHostInformationResourceUsage CollectInformationResourceUsage();

  std::string CollectJson();
  std::string AsJson(const FunctionHostInformationIdentification& information_identification,
                     const FunctionHostInformationEnvironment& information_environment,
                     const FunctionHostInformationResources& information_resources) const;

 private:
  struct CpuInfo {
    size_t cpu_count;
    std::string cpu_model;
    std::string cpu_features;
  };

  FunctionHostInformationCollectorConfiguration config_;

  std::string Id() const;
  static std::string IpPrivate();
  std::string IpPublic() const;

  std::string OperatingSystemDetails() const;
  std::string FileSystemDetails() const;
  size_t BootTimeSeconds() const;
  size_t UptimeSeconds() const;

  CpuInfo CpuInformation() const;
  size_t RamSizeMb() const;

  SystemMemoryUsage GetSystemMemoryUsage() const;
  ProcessMemoryUsage GetProcessMemoryUsage() const;

  static std::optional<std::string> FindFirst(const std::string& regex_string, const std::string& search_string);
  static std::vector<std::vector<std::string>> FindAll(const std::string& regex_string, std::string search_string);
  static std::string ReadFileContent(const std::string& filename);
  static std::string ReadStdout(const std::string& command);
  static size_t GetKbMemorySizeFromFile(const std::string& attribute, const std::string& file_content);
};
}  // namespace skyrise
