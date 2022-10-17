#include "request_tracker.hpp"

#include "monitor.hpp"

namespace skyrise {

namespace {

constexpr std::string_view kRequestTrackerAllocationTag = "request_tracker";

std::string GetKey(const Aws::String& serviceName, const Aws::String& requestName) {
  std::string key;

  key.reserve(serviceName.size() + requestName.size() + 1 /* separator */);
  key.append(serviceName);
  key.push_back(':');
  key.append(requestName);

  return key;
}

}  // namespace

void RequestTracker::Install(Aws::SDKOptions* options) {
  options->monitoringOptions.customizedMonitoringFactory_create_fn.emplace_back(
      [request_tracker = shared_from_this()]() {
        return Aws::MakeUnique<RequestCountingMonitorFactory>(kRequestTrackerAllocationTag.data(), request_tracker);
      });
}

void RequestTracker::RegisterRequestSucceeded(const Aws::String& serviceName, const Aws::String& requestName) {
  const std::string key = GetKey(serviceName, requestName);
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  ++request_counters_[key].succeeded;
}

void RequestTracker::RegisterRequestFailed(const Aws::String& serviceName, const Aws::String& requestName) {
  const std::string key = GetKey(serviceName, requestName);
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  ++request_counters_[key].failed;
}

void RequestTracker::RegisterRequestFinished(const Aws::String& serviceName, const Aws::String& requestName) {
  const std::string key = GetKey(serviceName, requestName);
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  ++request_counters_[key].finished;
}

void RequestTracker::Reset() {
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  request_counters_.clear();
}

std::unordered_map<std::string, RequestTracker::Statistics> RequestTracker::GetRequests() const {
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  return request_counters_;
}

void RequestTracker::WriteSummaryToStream(std::ostream* stream) const {
  *stream << "Request"
          << "\t"
          << "#finished"
          << "\t"
          << "#succeeded"
          << "\t"
          << "#failed"
          << "\n";

  for (const auto& [key, statistics] : GetRequests()) {
    *stream << key << "\t" << statistics.finished << "\t" << statistics.succeeded << "\t" << statistics.failed << "\n";
  }
}

void RequestTracker::WriteSummaryToJson(Aws::Utils::Json::JsonValue* json) const {
  Aws::Utils::Json::JsonValue metering;

  for (const auto& [key, statistics] : GetRequests()) {
    metering.WithObject(key, Aws::Utils::Json::JsonValue()
                                 .WithInt64("finished", statistics.finished)
                                 .WithInt64("succeeded", statistics.succeeded)
                                 .WithInt64("failed", statistics.failed));
  }

  json->WithObject("metering", std::move(metering));
}

}  // namespace skyrise
