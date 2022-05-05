#pragma once

#include <memory>
#include <mutex>
#include <ostream>
#include <unordered_map>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

namespace skyrise {

class RequestTracker : public std::enable_shared_from_this<RequestTracker> {
 public:
  struct Statistics {
    /**
     * Number of successful (and billed) requests. Due to automatic retries, failures can occure multiple times per API
     * Call.
     */
    size_t succeeded = 0;

    /**
     * Number of failed requests. Due to automatic retries, failures can occure multiple times per API Call.
     */
    size_t failed = 0;

    /** Number of requests that failed or succeeded. This count is either increased when a request failed permanently or
     * succeeds.
     */
    size_t finished = 0;
  };

  void Install(Aws::SDKOptions* options);
  void Reset();
  std::unordered_map<std::string, Statistics> GetRequests() const;

  void WriteSummaryToStream(std::ostream* stream) const;
  void WriteSummaryToJson(Aws::Utils::Json::JsonValue* json) const;

 private:
  void RegisterRequestSucceeded(const Aws::String& serviceName, const Aws::String& requestName);
  void RegisterRequestFailed(const Aws::String& serviceName, const Aws::String& requestName);
  void RegisterRequestFinished(const Aws::String& serviceName, const Aws::String& requestName);

  mutable std::mutex counter_mutex_;
  std::unordered_map<std::string, Statistics> request_counters_;
  friend class RequestCountingMonitor;
};

}  // namespace skyrise
