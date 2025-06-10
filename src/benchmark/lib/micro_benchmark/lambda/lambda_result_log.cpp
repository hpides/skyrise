#include "lambda_result_log.hpp"

#include <regex>

#include <aws/core/utils/base64/Base64.h>

namespace skyrise {

LambdaResultLog::LambdaResultLog(const std::string& result_log_encoded) {
  const Aws::Utils::ByteBuffer result_log_chars = Aws::Utils::Base64::Base64().Decode(result_log_encoded);
  const std::string log_result_decoded(reinterpret_cast<char const*>(result_log_chars.GetUnderlyingData()),
                                       result_log_chars.GetLength());

  for (const auto& result_log_key : GetAllResultLogKeys()) {
    const std::regex metric_regex("REPORT[\\s\\S]+?" + ToString(result_log_key) + ": ([^\\s]+)");
    std::smatch metric_match;
    const bool is_match = std::regex_search(log_result_decoded, metric_match, metric_regex);

    if (is_match) {
      log_entries_.emplace(result_log_key, metric_match[1]);
    }
  }
}

size_t LambdaResultLog::GetBilledDurationMs() const {
  return std::stoull(log_entries_.at(LambdaResultLogKey::kBilledDuration));
}

double LambdaResultLog::GetDurationMs() const { return std::stod(log_entries_.at(LambdaResultLogKey::kDuration)); }

double LambdaResultLog::GetInitDurationMs() const {
  return std::stod(log_entries_.at(LambdaResultLogKey::kInitDuration));
}

size_t LambdaResultLog::GetMaxMemoryUsedMb() const {
  return std::stoull(log_entries_.at(LambdaResultLogKey::kMaxMemoryUsed));
}

size_t LambdaResultLog::GetMemorySize() const { return std::stoull(log_entries_.at(LambdaResultLogKey::kMemorySize)); }

const std::string& LambdaResultLog::GetRequestId() const { return log_entries_.at(LambdaResultLogKey::kRequestId); }

const std::string& LambdaResultLog::GetXrayTraceId() const { return log_entries_.at(LambdaResultLogKey::kXrayTraceId); }

bool LambdaResultLog::HasInitDuration() const {
  return log_entries_.find(LambdaResultLogKey::kInitDuration) != log_entries_.cend();
}

bool LambdaResultLog::HasXrayTraceId() const {
  return log_entries_.find(LambdaResultLogKey::kXrayTraceId) != log_entries_.cend();
}

std::vector<LambdaResultLogKey> LambdaResultLog::GetAllResultLogKeys() {
  return {LambdaResultLogKey::kBilledDuration, LambdaResultLogKey::kDuration,   LambdaResultLogKey::kInitDuration,
          LambdaResultLogKey::kMaxMemoryUsed,  LambdaResultLogKey::kMemorySize, LambdaResultLogKey::kRequestId,
          LambdaResultLogKey::kXrayTraceId};
}

std::string LambdaResultLog::ToString(const LambdaResultLogKey result_log_key) {
  switch (result_log_key) {
    case LambdaResultLogKey::kBilledDuration:
      return "Billed Duration";
    case LambdaResultLogKey::kDuration:
      return "Duration";
    case LambdaResultLogKey::kInitDuration:
      return "Init Duration";
    case LambdaResultLogKey::kMaxMemoryUsed:
      return "Max Memory Used";
    case LambdaResultLogKey::kMemorySize:
      return "Memory Size";
    case LambdaResultLogKey::kRequestId:
      return "RequestId";
    case LambdaResultLogKey::kXrayTraceId:
      return "XRAY TraceId";
  }

  return "";
}

}  // namespace skyrise
