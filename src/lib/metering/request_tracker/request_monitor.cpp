#include "request_monitor.hpp"

#include <iostream>
#include <string_view>

#include "constants.hpp"

namespace skyrise {

RequestMonitor::RequestMonitor(std::shared_ptr<RequestTracker> tracker) : tracker_(std::move(tracker)) {}

void* RequestMonitor::OnRequestStarted(const Aws::String& /*serviceName*/, const Aws::String& /*requestName*/,
                                       const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/) const {
  return nullptr;
}

void RequestMonitor::OnRequestSucceeded(const Aws::String& service_name, const Aws::String& request_name,
                                        const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/,
                                        const Aws::Client::HttpResponseOutcome& /*outcome*/,
                                        const Aws::Monitoring::CoreMetricsCollection& /*metricsFromCore*/,
                                        void* /*context*/) const {
  // At this point we have access to the request.
  tracker_->RegisterRequestSucceeded(service_name, request_name);
}

void RequestMonitor::OnRequestFailed(const Aws::String& service_name, const Aws::String& request_name,
                                     const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/,
                                     const Aws::Client::HttpResponseOutcome& /*outcome*/,
                                     const Aws::Monitoring::CoreMetricsCollection& /*metricsFromCore*/,
                                     void* /*context*/) const {
  tracker_->RegisterRequestFailed(service_name, request_name);
}

void RequestMonitor::OnRequestRetry(const Aws::String& service_name, const Aws::String& request_name,
                                    const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/,
                                    void* /*context*/) const {
  tracker_->RegisterRequestRetried(service_name, request_name);
}

void RequestMonitor::OnFinish(const Aws::String& service_name, const Aws::String& request_name,
                              const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/,
                              void* /*context*/) const {
  tracker_->RegisterRequestFinished(service_name, request_name);
}

RequestMonitorFactory::RequestMonitorFactory(std::shared_ptr<RequestTracker> tracker) : tracker_(std::move(tracker)) {}

Aws::UniquePtr<Aws::Monitoring::MonitoringInterface> RequestMonitorFactory::CreateMonitoringInstance() const {
  // We cannot move here, because this method could - in theory - be called multiple times.
  // Also, we need a special Aws::UniquePtr here, since an allocation tag is required.
  return Aws::MakeUnique<RequestMonitor>(kBaseTag.data(), tracker_);
}

}  // namespace skyrise
