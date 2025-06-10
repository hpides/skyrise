#pragma once

#include <chrono>
#include <string>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/sqs/SQSClient.h>
#include <magic_enum/magic_enum.hpp>

#include "monitoring/monitoring_types.hpp"
#include "types.hpp"

namespace skyrise {

struct RuntimeMetrics {
  Aws::Utils::Json::JsonValue ToJson() const {
    Aws::Utils::Json::JsonValue metrics;

    if (processed_bytes > 0) {
      metrics.WithInt64("processed_bytes", processed_bytes);
    }
    if (processed_chunks > 0) {
      metrics.WithInt64("processed_chunks", processed_chunks);
    }
    if (processed_rows > 0) {
      metrics.WithInt64("processed_rows", processed_rows);
    }

    return metrics;
  }

  size_t processed_bytes;
  size_t processed_chunks;
  size_t processed_rows;
};

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class MetricsCollector {
 public:
  MetricsCollector(std::shared_ptr<const Aws::SQS::SQSClient> sqs_client, const std::string& queue_url,
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

  const std::shared_ptr<const Aws::SQS::SQSClient> sqs_client_;
  const std::string queue_url_;
  const SubqueryFragmentIdentifier subquery_fragment_identifier_;
  const std::chrono::time_point<std::chrono::system_clock> instance_start_;

  static constexpr size_t kMaxMessagesPerBatch = 10;
};

}  // namespace skyrise
