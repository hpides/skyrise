#include "log_result.hpp"

#include <regex>

#include <aws/core/Aws.h>
#include <aws/core/utils/base64/Base64.h>

namespace skyrise {

LogResult::LogResult(const std::string& log_result_encoded) {
  const Aws::Utils::ByteBuffer log_result_chars = Aws::Utils::Base64::Base64().Decode(log_result_encoded);
  const std::string log_result_decoded(reinterpret_cast<char const*>(log_result_chars.GetUnderlyingData()),
                                       log_result_chars.GetLength());

  for (const auto& log_result_key : GetAllLogResultKeys()) {
    const std::regex metric_regex("REPORT.+?" + ToString(log_result_key) + ": ([^\\s]+)");
    std::smatch metric_match;
    const bool is_match = std::regex_search(log_result_decoded, metric_match, metric_regex);

    if (is_match) {
      log_entries_.emplace(log_result_key, metric_match[1]);
    }
  }
}

size_t LogResult::GetBilledDurationMs() const { return std::stoull(log_entries_.at(LogResultKey::kBilledDuration)); }

double LogResult::GetDurationMs() const { return std::stod(log_entries_.at(LogResultKey::kDuration)); }

double LogResult::GetInitDurationMs() const { return std::stod(log_entries_.at(LogResultKey::kInitDuration)); }

size_t LogResult::GetMaxMemoryUsedMb() const { return std::stoull(log_entries_.at(LogResultKey::kMaxMemoryUsed)); }

size_t LogResult::GetMemorySize() const { return std::stoull(log_entries_.at(LogResultKey::kMemorySize)); }

const std::string& LogResult::GetRequestId() const { return log_entries_.at(LogResultKey::kRequestId); }

const std::string& LogResult::GetXrayTraceId() const { return log_entries_.at(LogResultKey::kXrayTraceId); }

bool LogResult::HasInitDuration() const {
  return log_entries_.find(LogResultKey::kInitDuration) != log_entries_.cend();
}

bool LogResult::HasXrayTraceId() const { return log_entries_.find(LogResultKey::kXrayTraceId) != log_entries_.cend(); }

std::vector<LogResultKey> LogResult::GetAllLogResultKeys() {
  return {LogResultKey::kBilledDuration, LogResultKey::kDuration,   LogResultKey::kInitDuration,
          LogResultKey::kMaxMemoryUsed,  LogResultKey::kMemorySize, LogResultKey::kRequestId,
          LogResultKey::kXrayTraceId};
}

std::string LogResult::ToString(const LogResultKey log_result_key) {
  switch (log_result_key) {
    case LogResultKey::kBilledDuration:
      return "Billed Duration";
    case LogResultKey::kDuration:
      return "Duration";
    case LogResultKey::kInitDuration:
      return "Init Duration";
    case LogResultKey::kMaxMemoryUsed:
      return "Max Memory Used";
    case LogResultKey::kMemorySize:
      return "Memory Size";
    case LogResultKey::kRequestId:
      return "RequestId";
    case LogResultKey::kXrayTraceId:
      return "XRAY TraceId";
  }

  return "";
}

}  // namespace skyrise
