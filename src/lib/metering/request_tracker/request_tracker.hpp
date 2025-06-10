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
     * Number of failed requests. Due to automatic retries in the AWS SDK, multiple failures may occur per API call.
     */
    size_t failed = 0;

    /** Number of requests that either failed or succeeded. This count is increased when a request failed permanently or
     * when it succeeded.
     */
    size_t finished = 0;

    /** Number of retried requests. This count is increased when a request had a backoff or timeout error.
     */
    size_t retried = 0;

    /**
     * Number of successful (and billed) requests.
     */
    size_t succeeded = 0;
  };

  void Install(Aws::SDKOptions* options);
  void Reset();
  std::unordered_map<std::string, Statistics> GetRequests() const;

  void WriteSummaryToStream(std::ostream* stream) const;
  void WriteSummaryToJson(Aws::Utils::Json::JsonValue* json) const;

  void RegisterRequestFailed(const Aws::String& service_name, const Aws::String& request_name);
  void RegisterRequestFinished(const Aws::String& service_name, const Aws::String& request_name);
  void RegisterRequestRetried(const Aws::String& service_name, const Aws::String& request_name);
  void RegisterRequestSucceeded(const Aws::String& service_name, const Aws::String& request_name);

 private:
  mutable std::mutex counter_mutex_;
  std::unordered_map<std::string, Statistics> request_counters_;
  friend class RequestMonitor;
};

}  // namespace skyrise
