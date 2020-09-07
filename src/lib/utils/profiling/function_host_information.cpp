#include "function_host_information.hpp"

#include <fstream>
#include <regex>

#include <aws/core/utils/json/JsonSerializer.h>

#include "utils/unit_conversion.hpp"

namespace skyrise {
FunctionHostInformationCollector::FunctionHostInformationCollector(FunctionHostInformationCollectorConfiguration config)
    : _config(config) {}

FunctionHostInformationIdentification FunctionHostInformationCollector::collect_information_identification() {
  return FunctionHostInformationIdentification{_id(), _ip_private(), _ip_public()};
}

FunctionHostInformationEnvironment FunctionHostInformationCollector::collect_information_environment() {
  return FunctionHostInformationEnvironment{_operating_system_details(), _file_system_details(), _boot_time_seconds(),
                                            _uptime_seconds()};
}

FunctionHostInformationResources FunctionHostInformationCollector::collect_information_resources() {
  const auto cpu_info = _cpu_information();
  return FunctionHostInformationResources{cpu_info.cpu_count, cpu_info.cpu_model, cpu_info.cpu_features,
                                          _ram_size_mb()};
}

std::string FunctionHostInformationCollector::collect_json() {
  const auto information_identification = collect_information_identification();
  const auto information_environment = collect_information_environment();
  const auto information_resources = collect_information_resources();
  return as_json(information_identification, information_environment, information_resources);
}

std::string FunctionHostInformationCollector::as_json(FunctionHostInformationIdentification information_identification,
                                                      FunctionHostInformationEnvironment information_environment,
                                                      FunctionHostInformationResources information_resources) {
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
  if (_config.readableJson) {
    return root.View().WriteReadable();
  }
  return root.View().WriteCompact();
}

std::string FunctionHostInformationCollector::_id() {
  constexpr auto REGEX = "[0-9]+:cpu:/sandbox-root-([0-9a-zA-Z]{6})";
  const auto file_content = _read_file_content(_config.cgroup_path);
  const auto match = _find_first(REGEX, file_content);
  return match.empty() ? "" : match.front();
}

std::string FunctionHostInformationCollector::_ip_private() {
  constexpr auto REGEX = "([0-9]+.[0-9]+.[0-9]+.[0-9]+)";
  const auto command_output = _read_stdout(_config.ip_private_command);
  const auto match = _find_first(REGEX, command_output);
  return match.empty() ? "" : match.front();
}

std::string FunctionHostInformationCollector::_ip_public() {
  if (!_config.collect_ip_public) {
    return "";
  }
  constexpr auto REGEX = "([0-9]+.[0-9]+.[0-9]+.[0-9]+)";
  const auto command_output = _read_stdout(_config.ip_public_command);
  const auto match = _find_first(REGEX, command_output);
  return match.empty() ? "" : match.front();
}

std::string FunctionHostInformationCollector::_operating_system_details() {
  auto command_output = _read_stdout(_config.operating_system_details_command);

  if (!command_output.empty() && command_output.back() == '\n') {
    command_output.pop_back();
  }
  return command_output;
}

std::string FunctionHostInformationCollector::_file_system_details() {
  auto command_output = _read_stdout(_config.ls_command + " " + _config.tmp_path);

  if (!command_output.empty() && command_output.back() == '\n') {
    command_output.pop_back();
  }
  return command_output;
}

size_t FunctionHostInformationCollector::_boot_time_seconds() {
  constexpr auto REGEX = "btime ([^[:space:]]*)";
  const auto file_content = _read_file_content(_config.stat_path);
  const auto match = _find_first(REGEX, file_content);
  return match.empty() ? 0 : stoul(match.front());
}

size_t FunctionHostInformationCollector::_uptime_seconds() {
  constexpr auto REGEX = "([0-9]*).[0-9]{2} [0-9]*.[0-9]{2}";
  const auto file_content = _read_file_content(_config.uptime_path);
  const auto match = _find_first(REGEX, file_content);
  return match.empty() ? 0 : stoul(match.front());
}

FunctionHostInformationCollector::_CpuInfo FunctionHostInformationCollector::_cpu_information() {
  const auto file_content = _read_file_content(_config.cpuinfo_path);

  constexpr auto CPU_COUNT_REGEX = "processor\\s+:\\s([0-9]+)\\n";
  const auto cpu_count_matches = _find_all(CPU_COUNT_REGEX, file_content);
  const auto cpu_count = cpu_count_matches.size();

  constexpr auto CPU_MODEL_REGEX = "model name\\s+:\\s(.+)\\n";
  const auto cpu_model_match = _find_first(CPU_MODEL_REGEX, file_content);
  const auto cpu_model = cpu_model_match.empty() ? "" : cpu_model_match.front();

  constexpr auto CPU_FEATURES_REGEX = "flags\\s+:\\s(.+)\\n";
  const auto cpu_features_match = _find_first(CPU_FEATURES_REGEX, file_content);
  const auto cpu_features = cpu_features_match.empty() ? "" : cpu_features_match.front();

  return {cpu_count, cpu_model, cpu_features};
}

size_t FunctionHostInformationCollector::_ram_size_mb() {
  constexpr auto REGEX = "MemTotal:\\s+([0-9]+)(\\skB)?\\n";
  const auto file_content = _read_file_content(_config.meminfo_path);
  const auto match = _find_first(REGEX, file_content);
  const auto ram_size_kb = match.empty() ? 0 : stoul(match.front());
  return ByteToMb(KbToByte(ram_size_kb));
}

std::vector<std::string> FunctionHostInformationCollector::_find_first(const std::string& regex_string,
                                                                       const std::string& search_string) {
  std::smatch matches;
  const std::regex regex(regex_string);
  regex_search(search_string, matches, regex);

  std::vector<std::string> result;
  for (const auto& match : matches) {
    result.push_back(match);
  }
  result.erase(result.begin());
  return result;
}

std::vector<std::vector<std::string>> FunctionHostInformationCollector::_find_all(const std::string& regex_string,
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

std::string FunctionHostInformationCollector::_read_file_content(const std::string& filename) {
  std::ifstream file(filename);
  if (file.fail()) {
    throw std::runtime_error("File '" + filename + "' could not be opened.");
  }
  std::string file_content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  return file_content;
}

std::string FunctionHostInformationCollector::_read_stdout(const std::string& command) {
  auto pipe{popen(command.c_str(), "r")};
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
};  // namespace skyrise
