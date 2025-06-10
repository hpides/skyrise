#include "monitoring/metrics_collector.hpp"

#include <chrono>
#include <thread>

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageBatchRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <gtest/gtest.h>

#include "client/base_client.hpp"
#include "monitoring/monitoring_types.hpp"
#include "testing/aws_test.hpp"
#include "utils/assert.hpp"

namespace skyrise {

class AwsMetricsCollectorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<BaseClient>();

    Aws::SQS::Model::CreateQueueRequest create_queue_request;
    create_queue_request.WithQueueName(queue_name_);
    const auto create_queue_outcome = client_->GetSqsClient()->CreateQueue(create_queue_request);
    queue_url_ = create_queue_outcome.GetResult().GetQueueUrl();

    Assert(create_queue_outcome.IsSuccess(), create_queue_outcome.GetError().GetMessage());
  }

  void TearDown() override {
    Aws::SQS::Model::DeleteQueueRequest delete_queue_request;
    delete_queue_request.WithQueueUrl(queue_url_);
    const auto delete_queue_outcome = client_->GetSqsClient()->DeleteQueue(delete_queue_request);

    Assert(delete_queue_outcome.IsSuccess(), delete_queue_outcome.GetError().GetMessage());
  }

  const AwsApi aws_api_;

  std::shared_ptr<BaseClient> client_;
  std::string queue_url_;

  static constexpr size_t kSleepSeconds = 3;
  inline static const std::string kPackageName = "skyriseSimpleFunction";
  inline static const std::string kRoleName = "AWSLambda";
  const std::string function_name_ = GetUniqueName(kPackageName);
  const std::string queue_name_ = GetUniqueName("skyriseTestQueue");
};

TEST_F(AwsMetricsCollectorTest, SendMetric) {
  {
    MetricsCollector metrics_collector(client_->GetSqsClient(), queue_url_, SubqueryFragmentIdentifier{});

    metrics_collector.CollectMetrics(RuntimeMetrics{});
    metrics_collector.EnterOperator("Operator 1");
    metrics_collector.CollectMetrics(RuntimeMetrics{});
    metrics_collector.EnterStage("Stage 1");
    metrics_collector.CollectMetrics(RuntimeMetrics{.processed_bytes = 1, .processed_chunks = 2, .processed_rows = 3});

    metrics_collector.EnterOperator("Operator 2");
    metrics_collector.EnterStage("Stage 1");
    metrics_collector.CollectMetrics(RuntimeMetrics{});
  }

  std::this_thread::sleep_for(std::chrono::seconds(kSleepSeconds));

  Aws::SQS::Model::ReceiveMessageRequest receive_message_request;
  receive_message_request.WithMaxNumberOfMessages(10).WithQueueUrl(queue_url_).WithWaitTimeSeconds(20);

  const auto outcome = client_->GetSqsClient()->ReceiveMessage(receive_message_request);
  const auto messages = outcome.GetResult().GetMessages();

  EXPECT_FALSE(messages.empty());

  for (const auto& message : messages) {
    const auto message_json = Aws::Utils::Json::JsonValue(message.GetBody());
    const auto metadata = message_json.View().GetObject("metadata");

    if (message_json.View().KeyExists("processed_bytes")) {
      EXPECT_STREQ(metadata.GetString("operator_id").c_str(), "Operator 1");
      EXPECT_STREQ(metadata.GetString("stage").c_str(), "Stage 1");

      EXPECT_EQ(message_json.View().GetInt64("processed_bytes"), 1);
      EXPECT_EQ(message_json.View().GetInt64("processed_chunks"), 2);
      EXPECT_EQ(message_json.View().GetInt64("processed_rows"), 3);
    } else {
      EXPECT_STREQ(metadata.GetString("operator_id").c_str(), "Operator 2");
      EXPECT_STREQ(metadata.GetString("stage").c_str(), "Stage 1");

      EXPECT_FALSE(message_json.View().KeyExists("processed_bytes"));
      EXPECT_FALSE(message_json.View().KeyExists("processed_chunks"));
      EXPECT_FALSE(message_json.View().KeyExists("processed_rows"));
    }
  }
}

}  // namespace skyrise
