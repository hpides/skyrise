#pragma once

#include <aws/sqs/SQSClient.h>

#include "configuration.hpp"

namespace skyrise {

/**
 * Utility function used to synchronize the tasks of multiple concurrently running instances, i.e., Lambda functions or
 * EC2 instances.
 * Every instance to be synchronized must call this function with a shared SQS queue. Once every instance registered
 * into the queue, this function will stop blocking and the task can be executed in parallel.
 */
void AwaitInstances(const std::string& sqs_queue_url, const std::shared_ptr<const Aws::SQS::SQSClient>& sqs_client,
                    const size_t concurrent_instance_count, const std::string& message);

}  // namespace skyrise
