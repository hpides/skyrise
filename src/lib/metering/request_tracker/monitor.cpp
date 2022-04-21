#include "monitor.hpp"

#include <iostream>
#include <string_view>

namespace skyrise {

namespace {
constexpr std::string_view kMonitoringAllocationTag = "skyrise_monitoring";
}  // namespace

RequestCountingMonitor::RequestCountingMonitor(std::shared_ptr<RequestTracker> tracker)
    : tracker_(std::move(tracker)) {}

void* RequestCountingMonitor::OnRequestStarted(const Aws::String& /*serviceName*/, const Aws::String& /*requestName*/,
                                               const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/) const {
  return nullptr;
}

void RequestCountingMonitor::OnRequestSucceeded(const Aws::String& serviceName, const Aws::String& requestName,
                                                const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/,
                                                const Aws::Client::HttpResponseOutcome& /*outcome*/,
                                                const Aws::Monitoring::CoreMetricsCollection& /*metricsFromCore*/,
                                                void* /*context*/) const {
  // At this point we have access to the request.
  tracker_->RegisterRequestSucceeded(serviceName, requestName);
}

void RequestCountingMonitor::OnRequestFailed(const Aws::String& serviceName, const Aws::String& requestName,
                                             const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/,
                                             const Aws::Client::HttpResponseOutcome& /*outcome*/,
                                             const Aws::Monitoring::CoreMetricsCollection& /*metricsFromCore*/,
                                             void* /*context*/) const {
  tracker_->RegisterRequestFailed(serviceName, requestName);
}

void RequestCountingMonitor::OnRequestRetry(const Aws::String& /*serviceName*/, const Aws::String& /*requestName*/,
                                            const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/,
                                            void* /*context*/) const {}

void RequestCountingMonitor::OnFinish(const Aws::String& serviceName, const Aws::String& requestName,
                                      const std::shared_ptr<const Aws::Http::HttpRequest>& /*request*/,
                                      void* /*context*/) const {
  tracker_->RegisterRequestFinished(serviceName, requestName);
}

RequestCountingMonitorFactory::RequestCountingMonitorFactory(std::shared_ptr<RequestTracker> tracker)
    : tracker_(std::move(tracker)) {}

Aws::UniquePtr<Aws::Monitoring::MonitoringInterface> RequestCountingMonitorFactory::CreateMonitoringInstance() const {
  // We cannot move here, because this method could - in theory - be called multiple times.
  // Also, we need a special Aws::UniquePtr here, since an allocation tag is required.
  return Aws::MakeUnique<RequestCountingMonitor>(kMonitoringAllocationTag.data(), tracker_);
}

}  // namespace skyrise
