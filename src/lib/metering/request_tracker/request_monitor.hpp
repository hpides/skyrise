#pragma once

#include <aws/core/Aws.h>
#include <aws/core/monitoring/MonitoringFactory.h>
#include <aws/core/monitoring/MonitoringInterface.h>

#include "request_tracker.hpp"

namespace skyrise {

class RequestMonitor : public Aws::Monitoring::MonitoringInterface {
 public:
  explicit RequestMonitor(std::shared_ptr<RequestTracker> tracker);
  void* OnRequestStarted(const Aws::String& service_name, const Aws::String& request_name,
                         const std::shared_ptr<const Aws::Http::HttpRequest>& request) const override;
  void OnRequestSucceeded(const Aws::String& service_name, const Aws::String& request_name,
                          const std::shared_ptr<const Aws::Http::HttpRequest>& request,
                          const Aws::Client::HttpResponseOutcome& outcome,
                          const Aws::Monitoring::CoreMetricsCollection& metrics_from_core,
                          void* context) const override;
  void OnRequestFailed(const Aws::String& service_name, const Aws::String& request_name,
                       const std::shared_ptr<const Aws::Http::HttpRequest>& request,
                       const Aws::Client::HttpResponseOutcome& outcome,
                       const Aws::Monitoring::CoreMetricsCollection& metrics_from_core, void* context) const override;
  void OnRequestRetry(const Aws::String& service_name, const Aws::String& request_name,
                      const std::shared_ptr<const Aws::Http::HttpRequest>& request, void* context) const override;
  void OnFinish(const Aws::String& service_name, const Aws::String& request_name,
                const std::shared_ptr<const Aws::Http::HttpRequest>& request, void* context) const override;

 private:
  std::shared_ptr<RequestTracker> tracker_;
};

class RequestMonitorFactory : public Aws::Monitoring::MonitoringFactory {
 public:
  explicit RequestMonitorFactory(std::shared_ptr<RequestTracker> tracker);
  Aws::UniquePtr<Aws::Monitoring::MonitoringInterface> CreateMonitoringInstance() const override;

 private:
  std::shared_ptr<RequestTracker> tracker_;
};

}  // namespace skyrise
