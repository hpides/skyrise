#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace skyrise {

enum class LambdaResultLogKey : std::uint8_t {
  kBilledDuration,
  kDuration,
  kInitDuration,
  kMaxMemoryUsed,
  kMemorySize,
  kRequestId,
  kXrayTraceId
};

class LambdaResultLog {
 public:
  explicit LambdaResultLog(const std::string& result_log_encoded);

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
  static std::vector<LambdaResultLogKey> GetAllResultLogKeys();
  static std::string ToString(const LambdaResultLogKey result_log_key);

  std::unordered_map<LambdaResultLogKey, std::string> log_entries_;
};

}  // namespace skyrise
