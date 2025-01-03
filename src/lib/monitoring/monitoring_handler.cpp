#include "monitoring_handler.hpp"

void skyrise::MonitoringHandler::EnterOperator(const std::string& operator_id) {
  metrics_collector_->EnterOperator(operator_id);
  tracer_->EnterOperator(operator_id);
  AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Entered operator with ID " + operator_id);
}
