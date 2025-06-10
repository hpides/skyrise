#include "request_tracker.hpp"

#include "constants.hpp"
#include "request_monitor.hpp"

namespace skyrise {

namespace {

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
        return Aws::MakeUnique<RequestMonitorFactory>(kBaseTag.data(), request_tracker);
      });
}

void RequestTracker::Reset() {
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  request_counters_.clear();
}

void RequestTracker::WriteSummaryToStream(std::ostream* stream) const {
  *stream << "Request"
          << "\t"
          << "#finished"
          << "\t"
          << "#succeeded"
          << "\t"
          << "#retried"
          << "\t"
          << "#failed"
          << "\n";

  for (const auto& [key, statistics] : GetRequests()) {
    *stream << key << "\t" << statistics.finished << "\t" << statistics.succeeded << "\t" << statistics.retried << "\t"
            << statistics.failed << "\n";
  }
}

void RequestTracker::WriteSummaryToJson(Aws::Utils::Json::JsonValue* json) const {
  Aws::Utils::Json::JsonValue metering;

  for (const auto& [key, statistics] : GetRequests()) {
    metering.WithObject(key, Aws::Utils::Json::JsonValue()
                                 .WithInt64(kResponseMeteringFinishedAttribute, statistics.finished)
                                 .WithInt64(kResponseMeteringSucceededAttribute, statistics.succeeded)
                                 .WithInt64(kResponseMeteringRetriedAttribute, statistics.retried)
                                 .WithInt64(kResponseMeteringFailedAttribute, statistics.failed));
  }

  json->WithObject(kResponseMeteringAttribute, std::move(metering));
}

std::unordered_map<std::string, RequestTracker::Statistics> RequestTracker::GetRequests() const {
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  return request_counters_;
}

void RequestTracker::RegisterRequestFailed(const Aws::String& service_name, const Aws::String& request_name) {
  const std::string key = GetKey(service_name, request_name);
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  ++request_counters_[key].failed;
}

void RequestTracker::RegisterRequestFinished(const Aws::String& service_name, const Aws::String& request_name) {
  const std::string key = GetKey(service_name, request_name);
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  ++request_counters_[key].finished;
}

void RequestTracker::RegisterRequestRetried(const Aws::String& service_name, const Aws::String& request_name) {
  const std::string key = GetKey(service_name, request_name);
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  ++request_counters_[key].retried;
}

void RequestTracker::RegisterRequestSucceeded(const Aws::String& service_name, const Aws::String& request_name) {
  const std::string key = GetKey(service_name, request_name);
  const std::lock_guard<std::mutex> lock(counter_mutex_);

  ++request_counters_[key].succeeded;
}

}  // namespace skyrise
