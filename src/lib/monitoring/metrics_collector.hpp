#pragma once

#include <chrono>
#include <string>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/sqs/SQSClient.h>
#include <magic_enum.hpp>

#include "scheduler/scheduler_types.hpp"

namespace skyrise {

struct RuntimeMetrics {
  size_t processed_bytes;
  size_t processed_chunks;
  size_t processed_rows;

  Aws::Utils::Json::JsonValue ToJson() const {
    Aws::Utils::Json::JsonValue metrics;

    if (processed_bytes > 0) metrics.WithInt64("processed_bytes", processed_bytes);
    if (processed_chunks > 0) metrics.WithInt64("processed_chunks", processed_chunks);
    if (processed_rows > 0) metrics.WithInt64("processed_rows", processed_rows);

    return metrics;
  }
};

class MetricsCollector {
 public:
  MetricsCollector(const Aws::SQS::SQSClient& client_sqs, const std::string& queue_url,
                   const SubqueryFragmentIdentifier& subquery_fragment_identifier);
  ~MetricsCollector();

  void EnterStage(const std::string& stage);
  void EnterOperator(const std::string& operator_id);

  void CollectMetrics(const RuntimeMetrics& runtime_metrics);

 private:
  Aws::Utils::Json::JsonValue CreateMessage(
      const std::pair<RuntimeMetrics, std::chrono::time_point<std::chrono::system_clock>>& runtime_metrics);
  void SendMetrics();

  std::vector<std::pair<RuntimeMetrics, std::chrono::time_point<std::chrono::system_clock>>> runtime_metrics_vector_;
  std::pair<std::string, std::chrono::time_point<std::chrono::system_clock>> operator_;
  std::pair<std::string, std::chrono::time_point<std::chrono::system_clock>> stage_;

  const Aws::SQS::SQSClient client_sqs_;
  const std::string queue_url_;
  const SubqueryFragmentIdentifier subquery_fragment_identifier_;
  const std::chrono::time_point<std::chrono::system_clock> instance_start_;

  inline static const std::string kTag{"SKYRISE/MONITORING/METRICS_COLLECTOR"};
  static constexpr size_t kMaxMessagesPerBatch = 10;
};

}  // namespace skyrise
