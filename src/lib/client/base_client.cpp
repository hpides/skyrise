#include "base_client.hpp"

#include <future>

#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/threading/Executor.h>

#include "configuration.hpp"
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

BaseClient::BaseClient() {
  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();
  if (!credentials_provider || (*credentials_provider).GetAWSCredentials().IsExpiredOrEmpty()) {
    Fail("AWS credentials are missing or expired. Please export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.\n");
  }

  const auto endpoint_provider = std::make_shared<Aws::S3::S3EndpointProvider>();

  auto client_configuration = GenerateClientConfig();
  client_region_ = client_configuration.region;

  auto client_configuration_dynamodb = GenerateClientConfig();
  client_configuration_dynamodb.retryStrategy = std::make_shared<CustomRetryStrategy>();

  auto client_configuration_s3 = GenerateClientConfig();
  client_configuration_s3.executor = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(
      FunctionHostInformationCollector().CollectInformationResources().cpu_count * kIoThreadPoolToCpuRatio);
  // client_configuration_s3.retryStrategy = std::make_shared<S3RetryStrategy>();
  // if (GetAwsRegion() == client_region_) {
  //   client_configuration_s3.httpRequestTimeoutMs = kS3RequestTimeoutMs;
  // }

  const std::vector<std::function<void()>> initializers{
      [&]() {
        dynamodb_client_ =
            std::make_shared<const Aws::DynamoDB::DynamoDBClient>(credentials_provider, client_configuration_dynamodb);
      },
      [&]() { efs_client_ = std::make_shared<const Aws::EFS::EFSClient>(credentials_provider, client_configuration); },
      [&]() {
        lambda_client_ = std::make_shared<const Aws::Lambda::LambdaClient>(credentials_provider, client_configuration);
      },
      [&]() {
        s3_client_ =
            std::make_shared<const Aws::S3::S3Client>(credentials_provider, endpoint_provider, client_configuration_s3);
      },
      [&]() { sqs_client_ = std::make_shared<const Aws::SQS::SQSClient>(credentials_provider, client_configuration); }};

  std::vector<std::future<void>> client_futures;
  client_futures.reserve(initializers.size());

  for (const auto& initializer : initializers) {
    client_futures.emplace_back(std::async(initializer));
  }

  for (const auto& client_future : client_futures) {
    client_future.wait();
  }
}

std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> BaseClient::GetDynamoDbClient() const { return dynamodb_client_; }

std::shared_ptr<const Aws::EFS::EFSClient> BaseClient::GetEfsClient() const { return efs_client_; }

std::shared_ptr<const Aws::Lambda::LambdaClient> BaseClient::GetLambdaClient() const { return lambda_client_; }

std::shared_ptr<const Aws::S3::S3Client> BaseClient::GetS3Client() const { return s3_client_; }

std::shared_ptr<const Aws::SQS::SQSClient> BaseClient::GetSqsClient() const { return sqs_client_; }

const Aws::String& BaseClient::GetClientRegion() const { return client_region_; }

Aws::Client::ClientConfiguration BaseClient::GenerateClientConfig() {
  Aws::Client::ClientConfiguration client_configuration;
  client_configuration.scheme = kHttpScheme;
  client_configuration.maxConnections = kMaxConnections;
  client_configuration.requestTimeoutMs = kRequestTimeoutMs;
  client_configuration.enableTcpKeepAlive = kEnableTcpKeepAlive;
  client_configuration.verifySSL = kVerifySsl;
  client_configuration.caFile = kCaFile;

  return client_configuration;
}

}  // namespace skyrise
