#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace skyrise {

enum class LogResultKey {
  kBilledDuration,
  kDuration,
  kInitDuration,
  kMaxMemoryUsed,
  kMemorySize,
  kRequestId,
  kXrayTraceId
};

class LogResult {
 public:
  LogResult(const std::string& log_result_encoded);

  size_t GetBilledDurationMs() const;
  double GetDurationMs() const;
  double GetInitDurationMs() const;
  size_t GetMaxMemoryUsedMb() const;
  size_t GetMemorySize() const;
  const std::string& GetRequestId() const;
  const std::string& GetXrayTraceId() const;

  bool HasInitDuration() const;
  bool HasXrayTraceId() const;

 private:
  static std::vector<LogResultKey> GetAllLogResultKeys();
  static std::string ToString(const LogResultKey log_result_key);

  std::unordered_map<LogResultKey, std::string> log_entries_;
};

}  // namespace skyrise
