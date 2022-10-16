#include "client.hpp"

#include <future>

#include <aws/core/utils/threading/Executor.h>

#include "utils/assert.hpp"

namespace skyrise {

Client::Client() {
  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();

  if (!credentials_provider || (*credentials_provider).GetAWSCredentials().IsExpiredOrEmpty()) {
    Fail("AWS credentials are missing or expired. Please export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.\n");
  }

  const auto client_configuration = GenerateClientConfig();
  client_region_ = client_configuration.region;

  // The Pricing API does not have endpoints in every region and is therefore always initialized with us-east-1.
  auto client_configuration_pricing = GenerateClientConfig();
  client_configuration_pricing.region = kPricingEndpoint;

  // We restrict the request rate for S3 via a PooledThreadExecutor in order to comply with AWS request limits.
  auto client_configuration_s3 = GenerateClientConfig();
  client_configuration_s3.executor = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(kS3ExecutorPoolSize);

  const std::vector<std::function<void()>> initializers{
      [&]() {
        cloudwatch_client_ =
            std::make_shared<const Aws::CloudWatch::CloudWatchClient>(credentials_provider, client_configuration);
      },
      [&]() {
        dynamodb_client_ =
            std::make_shared<const Aws::DynamoDB::DynamoDBClient>(credentials_provider, client_configuration);
      },
      [&]() { ec2_client_ = std::make_shared<const Aws::EC2::EC2Client>(credentials_provider, client_configuration); },
      [&]() {
        glue_client_ = std::make_shared<const Aws::Glue::GlueClient>(credentials_provider, client_configuration);
      },
      [&]() { iam_client_ = std::make_shared<const Aws::IAM::IAMClient>(credentials_provider, client_configuration); },
      [&]() {
        lambda_client_ = std::make_shared<const Aws::Lambda::LambdaClient>(credentials_provider, client_configuration);
      },
      [&]() {
        pricing_client_ =
            std::make_shared<const Aws::Pricing::PricingClient>(credentials_provider, client_configuration_pricing);
      },
      [&]() { s3_client_ = std::make_shared<const Aws::S3::S3Client>(credentials_provider, client_configuration_s3); },
      [&]() { sqs_client_ = std::make_shared<const Aws::SQS::SQSClient>(credentials_provider, client_configuration); },
      [&]() {
        xray_client_ = std::make_shared<const Aws::XRay::XRayClient>(credentials_provider, client_configuration);
      }};

  std::vector<std::future<void>> client_futures;
  client_futures.reserve(initializers.size());

  for (const auto& initializer : initializers) {
    client_futures.emplace_back(std::async(initializer));
  }

  for (const auto& client_future : client_futures) {
    client_future.wait();
  }
}

std::shared_ptr<const Aws::CloudWatch::CloudWatchClient> Client::GetCloudWatchClient() const {
  return cloudwatch_client_;
}

std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> Client::GetDynamoDbClient() const { return dynamodb_client_; }

std::shared_ptr<const Aws::EC2::EC2Client> Client::GetEc2Client() const { return ec2_client_; }

std::shared_ptr<const Aws::Glue::GlueClient> Client::GetGlueClient() const { return glue_client_; }

std::shared_ptr<const Aws::IAM::IAMClient> Client::GetIamClient() const { return iam_client_; }

std::shared_ptr<const Aws::Lambda::LambdaClient> Client::GetLambdaClient() const { return lambda_client_; }

std::shared_ptr<const Aws::Pricing::PricingClient> Client::GetPricingClient() const { return pricing_client_; }

std::shared_ptr<const Aws::S3::S3Client> Client::GetS3Client() const { return s3_client_; }

std::shared_ptr<const Aws::SQS::SQSClient> Client::GetSqsClient() const { return sqs_client_; }

std::shared_ptr<const Aws::XRay::XRayClient> Client::GetXRayClient() const { return xray_client_; }

const Aws::String& Client::GetClientRegion() const { return client_region_; }

Aws::Client::ClientConfiguration Client::GenerateClientConfig() {
  Aws::Client::ClientConfiguration client_configuration;
  client_configuration.caFile = kCaFile;
  client_configuration.connectTimeoutMs = kConnectTimeoutMs;
  client_configuration.enableTcpKeepAlive = kEnableTcpKeepAlive;
  client_configuration.maxConnections = kMaxConnections;
  client_configuration.requestTimeoutMs = kRequestTimeoutMs;

  return client_configuration;
}

}  // namespace skyrise
