#include "metrics_collector.hpp"

#include <cmath>

#include <aws/core/utils/logging/LogMacros.h>
#include <aws/sqs/model/SendMessageBatchRequest.h>

#include "constants.hpp"
#include "monitoring/monitoring_types.hpp"
#include "utils/string.hpp"

namespace skyrise {

MetricsCollector::MetricsCollector(std::shared_ptr<const Aws::SQS::SQSClient> sqs_client, const std::string& queue_url,
                                   const SubqueryFragmentIdentifier& subquery_fragment_identifier)
    : sqs_client_(std::move(sqs_client)),
      queue_url_(queue_url),
      subquery_fragment_identifier_(subquery_fragment_identifier),
      instance_start_(std::chrono::system_clock::now()) {}

MetricsCollector::~MetricsCollector() {
  if (!operator_.first.empty() && !stage_.first.empty()) {
    SendMetrics();
  }
}

void MetricsCollector::EnterOperator(const std::string& operator_id) {
  if (!stage_.first.empty()) {
    SendMetrics();
  }

  operator_ = {operator_id, std::chrono::system_clock::now()};
  stage_ = {};
}

void MetricsCollector::EnterStage(const std::string& stage) {
  if (operator_.first.empty()) {
    AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Operator not set.");
    return;
  }

  if (!stage_.first.empty()) {
    SendMetrics();
  }

  stage_ = {stage, std::chrono::system_clock::now()};
}

void MetricsCollector::CollectMetrics(const RuntimeMetrics& runtime_metrics) {
  if (operator_.first.empty() || stage_.first.empty()) {
    AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Operator or stage not set.");
    return;
  }

  runtime_metrics_vector_.emplace_back(runtime_metrics, std::chrono::system_clock::now());
}

Aws::Utils::Json::JsonValue MetricsCollector::CreateMessage(
    const std::pair<RuntimeMetrics, std::chrono::time_point<std::chrono::system_clock>>& runtime_metrics) {
  auto metrics = runtime_metrics.first.ToJson();

  metrics.WithDouble("elapsed_time", (runtime_metrics.second - instance_start_).count())
      .WithDouble("elapsed_time_in_operator", (runtime_metrics.second - operator_.second).count())
      .WithDouble("elapsed_time_in_stage", (runtime_metrics.second - stage_.second).count());

  auto identifier = subquery_fragment_identifier_.ToJson();
  identifier.WithString("operator_id", operator_.first).WithString("stage", stage_.first);

  return metrics.WithObject("metadata", identifier);
}

void MetricsCollector::SendMetrics() {
  const size_t runtime_metrics_num = runtime_metrics_vector_.size();
  const double batches_num = runtime_metrics_num / static_cast<double>(kMaxMessagesPerBatch);
  std::vector<std::vector<Aws::SQS::Model::SendMessageBatchRequestEntry>> message_batches(std::ceil(batches_num));

  auto it_start = runtime_metrics_vector_.cbegin();

  for (size_t i = 0; i < kMaxMessagesPerBatch; ++i) {
    const auto upper_bound = static_cast<size_t>(batches_num * static_cast<double>(i + 1));
    const auto it_end = runtime_metrics_vector_.cbegin() + upper_bound;

    for (size_t j = 0; it_start != it_end; ++j) {
      message_batches[j].emplace_back(Aws::SQS::Model::SendMessageBatchRequestEntry()
                                          .WithId(RandomString(8))
                                          .WithMessageBody(CreateMessage(*it_start++).View().WriteCompact()));
    }

    it_start = it_end;
  }

  for (const auto& messages : message_batches) {
    Aws::SQS::Model::SendMessageBatchRequest send_message_batch_request;
    send_message_batch_request.WithQueueUrl(queue_url_).WithEntries(messages);
    const auto outcome = sqs_client_->SendMessageBatch(send_message_batch_request);

    if (!outcome.IsSuccess()) {
      AWS_LOGSTREAM_ERROR(kWorkerTag.c_str(), outcome.GetError().GetMessage());
    }
  }

  runtime_metrics_vector_.clear();
  runtime_metrics_vector_.shrink_to_fit();
}

}  // namespace skyrise
