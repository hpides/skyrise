#include "client_aws.hpp"

#include <future>

#include "utils/assert.hpp"

namespace skyrise {

ClientAws::ClientAws() {
  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();

  if (!credentials_provider || (*credentials_provider).GetAWSCredentials().IsExpiredOrEmpty()) {
    Fail("AWS credentials are missing or expired. Please export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.\n");
  }

  const auto client_configuration = GenerateClientConfig();
  client_region_ = client_configuration.region;

  // The Pricing API does not have endpoints in every region and is therefore always initialized with us-east-1
  auto client_configuration_pricing = GenerateClientConfig();
  client_configuration_pricing.region = kPricingEndpoint;

  std::vector<std::function<void()>> initializers{
      [&]() {
        cloudwatch_client_ =
            std::make_unique<Aws::CloudWatch::CloudWatchClient>(credentials_provider, client_configuration);
      },
      [&]() { iam_client_ = std::make_unique<Aws::IAM::IAMClient>(credentials_provider, client_configuration); },
      [&]() {
        lambda_client_ = std::make_unique<Aws::Lambda::LambdaClient>(credentials_provider, client_configuration);
      },
      [&]() {
        pricing_client_ =
            std::make_unique<Aws::Pricing::PricingClient>(credentials_provider, client_configuration_pricing);
      },
      [&]() { s3_client_ = std::make_unique<Aws::S3::S3Client>(credentials_provider, client_configuration); },
      [&]() { sqs_client_ = std::make_unique<Aws::SQS::SQSClient>(credentials_provider, client_configuration); },
      [&]() { xray_client_ = std::make_unique<Aws::XRay::XRayClient>(credentials_provider, client_configuration); }};

  std::vector<std::future<void>> client_futures;
  client_futures.reserve(initializers.size());

  for (const auto& initializer : initializers) {
    client_futures.emplace_back(std::async(initializer));
  }

  for (const auto& client_future : client_futures) {
    client_future.wait();
  }
}

const Aws::CloudWatch::CloudWatchClient& ClientAws::GetCloudWatchClient() const { return *cloudwatch_client_; }

const Aws::IAM::IAMClient& ClientAws::GetIAMClient() const { return *iam_client_; }

const Aws::Lambda::LambdaClient& ClientAws::GetLambdaClient() const { return *lambda_client_; }

const Aws::Pricing::PricingClient& ClientAws::GetPricingClient() const { return *pricing_client_; }

const Aws::S3::S3Client& ClientAws::GetS3Client() const { return *s3_client_; }

const Aws::SQS::SQSClient& ClientAws::GetSQSClient() const { return *sqs_client_; }

const Aws::XRay::XRayClient& ClientAws::GetXRayClient() const { return *xray_client_; }

const Aws::String& ClientAws::GetClientRegion() const { return client_region_; }

Aws::Client::ClientConfiguration ClientAws::GenerateClientConfig() const {
  Aws::Client::ClientConfiguration client_configuration;
  client_configuration.caFile = kCaFile;
  client_configuration.connectTimeoutMs = kConnectTimeoutMs;
  client_configuration.enableTcpKeepAlive = kEnableTcpKeepAlive;
  client_configuration.maxConnections = kMaxConnections;
  client_configuration.requestTimeoutMs = kRequestTimeoutMs;

  return client_configuration;
}

}  // namespace skyrise
