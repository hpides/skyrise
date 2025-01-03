#include "synchronize_instances.hpp"

#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>

namespace skyrise {

void AwaitInstances(const std::string& sqs_queue_url, const std::shared_ptr<const Aws::SQS::SQSClient>& sqs_client,
                    const size_t concurrent_instance_count, const std::string& message) {
  sqs_client->SendMessage(Aws::SQS::Model::SendMessageRequest().WithQueueUrl(sqs_queue_url).WithMessageBody(message));

  size_t queue_size = 0;
  Aws::SQS::Model::GetQueueAttributesRequest receive_request;
  receive_request.SetQueueUrl(sqs_queue_url);
  receive_request.AddAttributeNames(Aws::SQS::Model::QueueAttributeName::ApproximateNumberOfMessages);

  while (queue_size < concurrent_instance_count) {
    const auto outcome = sqs_client->GetQueueAttributes(receive_request);
    queue_size = stoi(outcome.GetResult()
                          .GetAttributes()
                          .find(Aws::SQS::Model::QueueAttributeName::ApproximateNumberOfMessages)
                          ->second);
    std::this_thread::sleep_for(std::chrono::milliseconds(kSqsApproximateQueueSizePollIntervalMilliseconds));
  }
}

}  // namespace skyrise
