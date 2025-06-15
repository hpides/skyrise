#include "coordinator_client.hpp"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/sqs/SQSClient.h>

#include "configuration.hpp"
#include "constants.hpp"

namespace skyrise {

CoordinatorClient::CoordinatorClient() 
    : BaseClient("", "") {  // Initialize with empty credentials since we're using environment variables
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  const auto client_config = GenerateClientConfig();
  s3_client_ = std::make_shared<Aws::S3::S3Client>(client_config);
  sqs_client_ = std::make_shared<Aws::SQS::SQSClient>(client_config);
}

std::shared_ptr<const Aws::S3::S3Client> CoordinatorClient::getS3Client() const {
  return s3_client_;
}

std::shared_ptr<const Aws::SQS::SQSClient> CoordinatorClient::GetSqsClient() const {
  return sqs_client_;
}

}  // namespace skyrise
