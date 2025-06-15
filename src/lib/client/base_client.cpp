#include "base_client.hpp"

#include <future>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/threading/Executor.h>

#include "configuration.hpp"
#include "constants.hpp"
#include "storage/backend/s3_storage.hpp"
#include "storage/backend/s3_utils.hpp"
#include "utils/assert.hpp"
#include "utils/profiling/function_host_information.hpp"
#include "utils/region.hpp"

namespace {

class CustomRetryStrategy : public Aws::Client::DefaultRetryStrategy {
 public:
  explicit CustomRetryStrategy(long maxRetries = 15, long scaleFactor = 25)
      : DefaultRetryStrategy(maxRetries, scaleFactor) {}
};
}  // namespace

namespace skyrise {

BaseClient::BaseClient(const std::string& access_key_id, const std::string& secret_access_key)
    : access_key_id_(access_key_id), secret_access_key_(secret_access_key) {
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  Aws::Client::ClientConfiguration client_configuration;
  client_configuration.region = "auto";
  client_configuration.endpointOverride = kR2Endpoint;

  auto credentials_provider = Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>(
      "R2Credentials", access_key_id_, secret_access_key_);

  auto endpoint_provider = Aws::MakeShared<Aws::S3::S3EndpointProvider>("R2EndpointProvider");

  // Initialize S3 client for R2
  s3_client_ = std::make_shared<const Aws::S3::S3Client>(
      credentials_provider, endpoint_provider, client_configuration);
}

BaseClient::~BaseClient() = default;

std::shared_ptr<const Aws::S3::S3Client> BaseClient::getS3Client() const {
  return s3_client_;
}

// std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> BaseClient::GetDynamoDbClient() const { return dynamodb_client_; }

// std::shared_ptr<const Aws::EFS::EFSClient> BaseClient::GetEfsClient() const { return efs_client_; }

// std::shared_ptr<const Aws::Lambda::LambdaClient> BaseClient::GetLambdaClient() const { return lambda_client_; }

std::shared_ptr<const Aws::SQS::SQSClient> BaseClient::GetSqsClient() const {
  return sqs_client_;
}

const Aws::String& BaseClient::GetClientRegion() const {
  return client_region_;
}

Aws::Client::ClientConfiguration BaseClient::GenerateClientConfig() {
  Aws::Client::ClientConfiguration config;
  config.scheme = kHttpScheme;
  config.maxConnections = kMaxConnections;
  config.requestTimeoutMs = kRequestTimeoutMs;
  config.enableTcpKeepAlive = kEnableTcpKeepAlive;
  config.verifySSL = kVerifySsl;
  config.caFile = kCaFile;
  return config;
}

}  // namespace skyrise
