#pragma once

#include <aws/core/Aws.h>
#include <aws/core/monitoring/MonitoringFactory.h>
#include <aws/core/monitoring/MonitoringInterface.h>

#include "request_tracker.hpp"

namespace skyrise {

class RequestCountingMonitor : public Aws::Monitoring::MonitoringInterface {
 public:
  RequestCountingMonitor(std::shared_ptr<RequestTracker> tracker);
  virtual void* OnRequestStarted(const Aws::String& serviceName, const Aws::String& requestName,
                                 const std::shared_ptr<const Aws::Http::HttpRequest>& request) const override;
  virtual void OnRequestSucceeded(const Aws::String& serviceName, const Aws::String& requestName,
                                  const std::shared_ptr<const Aws::Http::HttpRequest>& request,
                                  const Aws::Client::HttpResponseOutcome& outcome,
                                  const Aws::Monitoring::CoreMetricsCollection& metricsFromCore,
                                  void* context) const override;
  virtual void OnRequestFailed(const Aws::String& serviceName, const Aws::String& requestName,
                               const std::shared_ptr<const Aws::Http::HttpRequest>& request,
                               const Aws::Client::HttpResponseOutcome& outcome,
                               const Aws::Monitoring::CoreMetricsCollection& metricsFromCore,
                               void* context) const override;
  virtual void OnRequestRetry(const Aws::String& serviceName, const Aws::String& requestName,
                              const std::shared_ptr<const Aws::Http::HttpRequest>& request,
                              void* context) const override;
  virtual void OnFinish(const Aws::String& serviceName, const Aws::String& requestName,
                        const std::shared_ptr<const Aws::Http::HttpRequest>& request, void* context) const override;

 private:
  std::shared_ptr<RequestTracker> tracker_;
};

class RequestCountingMonitorFactory : public Aws::Monitoring::MonitoringFactory {
 public:
  RequestCountingMonitorFactory(std::shared_ptr<RequestTracker> tracker);
  Aws::UniquePtr<Aws::Monitoring::MonitoringInterface> CreateMonitoringInstance() const override;

 private:
  std::shared_ptr<RequestTracker> tracker_;
};

}  // namespace skyrise
