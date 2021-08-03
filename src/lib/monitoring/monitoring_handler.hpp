#pragma once

#include <aws/core/utils/logging/LogMacros.h>

#include "client/client.hpp"
#include "metrics_collector.hpp"
#include "tracer.hpp"

namespace skyrise {

class MonitoringHandler {
 public:
  MonitoringHandler(std::shared_ptr<const Aws::SQS::SQSClient> sqs_client,
                    std::shared_ptr<const Aws::XRay::XRayClient> xray_client,
                    const SubqueryFragmentIdentifier& subquery_fragment_identifier, const std::string& xray_trace_id,
                    const std::string& queue_url)
      : metrics_collector_(
            std::make_shared<MetricsCollector>(std::move(sqs_client), queue_url, subquery_fragment_identifier)),
        tracer_(std::make_shared<Tracer>(std::move(xray_client), xray_trace_id, subquery_fragment_identifier)){};

  void EnterOperator(const std::string& operator_id);
  template <typename Stages>
  void EnterStage(const Stages& stage);

 private:
  const std::shared_ptr<MetricsCollector> metrics_collector_;
  const std::shared_ptr<Tracer> tracer_;

  inline static const std::string kTag{"SKYRISE/MONITORING/MONITORING_HANDLER"};
};

template <typename Stages>
void skyrise::MonitoringHandler::EnterStage(const Stages& stage) {
  const std::string stage_name = std::string(magic_enum::enum_name(stage));
  metrics_collector_->EnterStage(stage_name);
  tracer_->EnterStage(stage_name);
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Entered stage " + stage_name);
}

}  // namespace skyrise
