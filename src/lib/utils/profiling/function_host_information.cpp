#include "function_host_information.hpp"

#include <fstream>
#include <regex>

#include <aws/core/utils/json/JsonSerializer.h>

#include "utils/unit_conversion.hpp"

namespace skyrise {

NetworkInterface NetworkInterface::GetFirstInterface() {
  ifaddrs* interface_addresses = nullptr;
  if (getifaddrs(&interface_addresses) == 0) {
    return {std::shared_ptr<ifaddrs>(interface_addresses, [](ifaddrs* p) { freeifaddrs(p); })};
  }
  return {nullptr};
}

NetworkInterface NetworkInterface::Next() {
  return {interface_address_list_head_, current_interface_address_->ifa_next};
}

bool NetworkInterface::IsIpv6LinkLocalAddress() {
  auto* inet6_address = reinterpret_cast<sockaddr_in6*>(current_interface_address_->ifa_addr);
  return IN6_IS_ADDR_LINKLOCAL(&inet6_address->sin6_addr) ||   // NOLINT
         IN6_IS_ADDR_MC_LINKLOCAL(&inet6_address->sin6_addr);  // NOLINT
}

std::string NetworkInterface::GetNumericHostname() {
  std::array<char, 255> name_buffer{};
  const int address_struct_length = IsIpv4() ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);
  const int get_name_result = getnameinfo(current_interface_address_->ifa_addr, address_struct_length,
                                          name_buffer.data(), sizeof(name_buffer), nullptr, 0, NI_NUMERICHOST);

  // getnameinfo will return 0 for success
  return get_name_result == 0 ? name_buffer.data() : std::string();
}

FunctionHostInformationCollector::FunctionHostInformationCollector(
    const FunctionHostInformationCollectorConfiguration& config)
    : config_(config) {}

FunctionHostInformationIdentification FunctionHostInformationCollector::CollectInformationIdentification() {
  return {Id(), IpPrivate(), IpPublic()};
}

FunctionHostInformationEnvironment FunctionHostInformationCollector::CollectInformationEnvironment() {
  return {OperatingSystemDetails(), FileSystemDetails(), BootTimeSeconds(), UptimeSeconds()};
}

FunctionHostInformationResources FunctionHostInformationCollector::CollectInformationResources() {
  const auto cpu_info = CpuInformation();
  return {cpu_info.cpu_count, cpu_info.cpu_model, cpu_info.cpu_features, RamSizeMb()};
}

FunctionHostInformationResourceUsage FunctionHostInformationCollector::CollectInformationResourceUsage() {
  const SystemMemoryUsage system_memory = GetSystemMemoryUsage();
  const ProcessMemoryUsage process_memory = GetProcessMemoryUsage();

  return {process_memory.physical_memory_kb, process_memory.virtual_memory_kb, system_memory.available_memory_kb,
          system_memory.free_memory_kb};
}

std::string FunctionHostInformationCollector::CollectJson() {
  const auto information_identification = CollectInformationIdentification();
  const auto information_environment = CollectInformationEnvironment();
  const auto information_resources = CollectInformationResources();
  return AsJson(information_identification, information_environment, information_resources);
}

std::string FunctionHostInformationCollector::AsJson(
    const FunctionHostInformationIdentification& information_identification,
    const FunctionHostInformationEnvironment& information_environment,
    const FunctionHostInformationResources& information_resources) const {
  Aws::Utils::Json::JsonValue information_identification_json;
  information_identification_json.WithString("id", information_identification.id);
  information_identification_json.WithString("ip_private", information_identification.ip_private);
  information_identification_json.WithString("ip_public", information_identification.ip_public);

  Aws::Utils::Json::JsonValue information_environment_json;
  information_environment_json.WithString("operating_system_details", information_environment.operating_system_details);
  information_environment_json.WithString("file_system_details", information_environment.file_system_details);
  information_environment_json.WithInt64("boot_time_seconds", information_environment.boot_time_seconds);
  information_environment_json.WithInt64("uptime_seconds", information_environment.uptime_seconds);

  Aws::Utils::Json::JsonValue information_resources_json;
  information_resources_json.WithInt64("cpu_count", information_resources.cpu_count);
  information_resources_json.WithString("cpu_model", information_resources.cpu_model);
  information_resources_json.WithString("cpu_features", information_resources.cpu_features);
  information_resources_json.WithInt64("ram_size_mb", information_resources.ram_size_mb);

  Aws::Utils::Json::JsonValue root;
  root.WithObject("identification", information_identification_json);
  root.WithObject("environment", information_environment_json);
  root.WithObject("resources", information_resources_json);
  if (config_.readable_json) {
    return root.View().WriteReadable();
  }
  return root.View().WriteCompact();
}

std::string FunctionHostInformationCollector::Id() const {
  const std::string regex{"[0-9]+:cpu,cpuacct:/sandbox-root-([0-9a-zA-Z]{6})"};
  const auto file_content = ReadFileContent(config_.cgroup_path);
  const auto match = FindFirst(regex, file_content);
  return match ? *match : "";
}

std::string FunctionHostInformationCollector::IpPrivate() {
  NetworkInterface interface = NetworkInterface::GetFirstInterface();
  while (interface.IsValid()) {
    if (interface.HasAddress() && interface.IsUp() && !interface.IsLoopback() &&
        (interface.IsIpv4() || (interface.IsIpv6() && !interface.IsIpv6LinkLocalAddress()))) {
      return interface.GetNumericHostname();
    }
    interface = interface.Next();
  }

  return "";
}

std::string FunctionHostInformationCollector::IpPublic() const {
  if (!config_.collect_ip_public) {
    return "";
  }
  const std::string regex{"([0-9]+.[0-9]+.[0-9]+.[0-9]+)"};
  const auto command_output = ReadStdout(config_.ip_public_command);
  const auto match = FindFirst(regex, command_output);
  return match ? *match : "";
}

std::string FunctionHostInformationCollector::OperatingSystemDetails() const {
  auto command_output = ReadStdout(config_.operating_system_details_command);

  if (!command_output.empty() && command_output.back() == '\n') {
    command_output.pop_back();
  }
  return command_output;
}

std::string FunctionHostInformationCollector::FileSystemDetails() const {
  auto command_output = ReadStdout(config_.ls_command + " " + config_.tmp_path);

  if (!command_output.empty() && command_output.back() == '\n') {
    command_output.pop_back();
  }
  return command_output;
}

size_t FunctionHostInformationCollector::BootTimeSeconds() const {
  const std::string regex{"btime ([^[:space:]]*)"};
  const auto file_content = ReadFileContent(config_.stat_path);
  const auto match = FindFirst(regex, file_content);
  return match ? std::stoull(*match) : 0;
}

size_t FunctionHostInformationCollector::UptimeSeconds() const {
  const std::string regex{"([0-9]*).[0-9]{2} [0-9]*.[0-9]{2}"};
  const auto file_content = ReadFileContent(config_.uptime_path);
  const auto match = FindFirst(regex, file_content);
  return match ? std::stoull(*match) : 0;
}

FunctionHostInformationCollector::CpuInfo FunctionHostInformationCollector::CpuInformation() const {
  const auto file_content = ReadFileContent(config_.cpuinfo_path);

  constexpr auto kCpuCountRegex = R"(processor\s+:\s([0-9]+)\n)";
  const auto cpu_count_matches = FindAll(kCpuCountRegex, file_content);
  const auto cpu_count = cpu_count_matches.size();

  constexpr auto kCpuModelRegex = R"(model name\s+:\s(.+)\n)";
  const auto cpu_model_match = FindFirst(kCpuModelRegex, file_content);
  const auto cpu_model = cpu_model_match ? *cpu_model_match : "";

  constexpr auto kCpuFeaturesRegex = R"(flags\s+:\s(.+)\n)";
  const auto cpu_features_match = FindFirst(kCpuFeaturesRegex, file_content);
  const auto cpu_features = cpu_features_match ? *cpu_features_match : "";

  return {cpu_count, cpu_model, cpu_features};
}

size_t FunctionHostInformationCollector::RamSizeMb() const {
  const std::string regex{R"(MemTotal:\s+([0-9]+)(\skB)?\n)"};
  const auto file_content = ReadFileContent(config_.meminfo_path);
  const auto match = FindFirst(regex, file_content);
  const auto ram_size_kb = match ? std::stoull(*match) : 0;
  return KBToMB(ram_size_kb);
}

std::optional<std::string> FunctionHostInformationCollector::FindFirst(const std::string& regex_string,
                                                                       const std::string& search_string) {
  std::smatch matches;
  const std::regex regex(regex_string);
  regex_search(search_string, matches, regex);

  if (matches.size() >= 2) {
    return matches[1];
  } else {
    return std::nullopt;
  }
}

std::vector<std::vector<std::string>> FunctionHostInformationCollector::FindAll(const std::string& regex_string,
                                                                                std::string search_string) {
  std::smatch matches;
  const std::regex regex(regex_string);

  std::vector<std::vector<std::string>> results;
  while (regex_search(search_string, matches, regex)) {
    std::vector<std::string> result;
    for (const auto& match : matches) {
      result.push_back(match);
    }
    result.erase(result.begin());
    results.push_back(result);
    search_string = matches.suffix();
  }

  return results;
}

std::string FunctionHostInformationCollector::ReadFileContent(const std::string& filename) {
  std::ifstream file(filename);
  if (file.fail()) {
    throw std::runtime_error("File '" + filename + "' could not be opened.");
  }
  std::string file_content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  return file_content;
}

std::string FunctionHostInformationCollector::ReadStdout(const std::string& command) {
  auto* pipe{popen(command.c_str(), "r")};
  if (pipe == nullptr) {
    throw std::runtime_error("Could not run command: " + command);
  }

  std::string command_stdout;
  std::array<char, 128> buffer{};
  while (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
    command_stdout += buffer.data();
  }

  auto return_code = pclose(pipe);
  if (return_code != EXIT_SUCCESS) {
    throw std::runtime_error("Command " + command + " exited with return code " + std::to_string(return_code));
  }
  return command_stdout;
}

size_t FunctionHostInformationCollector::GetKbMemorySizeFromFile(const std::string& attribute,
                                                                 const std::string& file_content) {
  const auto match = FindFirst(attribute + R"(:\s+([0-9]+)\skB?\n)", file_content);
  return match ? std::stoull(*match) : -1;
}

/**
 * Returns a struct that contains the free and available memory size in KB.
 * - Free memory is unallocated memory.
 * - Available memory includes free memory and currently allocated memory that
 *   could be made available (e.g., buffers and caches).
 *   This is not equivalent to the total memory size, since certain data cannot
 *   be paged.
 */
SystemMemoryUsage FunctionHostInformationCollector::GetSystemMemoryUsage() const {
  const auto file_content = ReadFileContent(config_.meminfo_path);

  return {GetKbMemorySizeFromFile("MemAvailable", file_content), GetKbMemorySizeFromFile("MemFree", file_content)};
}

/**
 * Returns a struct that contains the virtual and physical memory used by this process in KB.
 * - Virtual memory is the total memory usage of the process.
 * - Physical memory is the resident set size (RSS), the portion of memory that is held in RAM.
 */
ProcessMemoryUsage FunctionHostInformationCollector::GetProcessMemoryUsage() const {
  const auto file_content = ReadFileContent(config_.self_status_path);

  return {GetKbMemorySizeFromFile("VmRSS", file_content), GetKbMemorySizeFromFile("VmSize", file_content)};
}

}  // namespace skyrise
