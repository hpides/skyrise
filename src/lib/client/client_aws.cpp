#include "client_aws.hpp"

#include "utils/assert.hpp"

namespace skyrise {

ClientAws::ClientAws() {
  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();

  if (!credentials_provider || (*credentials_provider).GetAWSCredentials().IsExpiredOrEmpty()) {
    Fail("AWS credentials are missing or expired. Please export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.\n");
  }

  Aws::Client::ClientConfiguration client_configuration;

  client_region_ = client_configuration.region;

  client_configuration.caFile = kCaFile;
  client_configuration.requestTimeoutMs = kRequestTimeoutMs;
  client_configuration.maxConnections = kMaxConnections;

  cloudwatch_client_ = Aws::CloudWatch::CloudWatchClient(credentials_provider, client_configuration);
  iam_client_ = Aws::IAM::IAMClient(credentials_provider, client_configuration);
  lambda_client_ = Aws::Lambda::LambdaClient(credentials_provider, client_configuration);
  s3_client_ = Aws::S3::S3Client(credentials_provider, client_configuration);
  sqs_client_ = Aws::SQS::SQSClient(credentials_provider, client_configuration);
  xray_client_ = Aws::XRay::XRayClient(credentials_provider, client_configuration);

  // The Pricing API does not have endpoints in every region and is therefore always initialized with us-east-1
  client_configuration.region = kPricingEndpoint;
  pricing_client_ = Aws::Pricing::PricingClient(credentials_provider, client_configuration);
}

const Aws::CloudWatch::CloudWatchClient& ClientAws::GetCloudWatchClient() const { return cloudwatch_client_; }

const Aws::IAM::IAMClient& ClientAws::GetIAMClient() const { return iam_client_; }

const Aws::Lambda::LambdaClient& ClientAws::GetLambdaClient() const { return lambda_client_; }

const Aws::Pricing::PricingClient& ClientAws::GetPricingClient() const { return pricing_client_; }

const Aws::S3::S3Client& ClientAws::GetS3Client() const { return s3_client_; }

const Aws::SQS::SQSClient& ClientAws::GetSQSClient() const { return sqs_client_; }

const Aws::XRay::XRayClient& ClientAws::GetXRayClient() const { return xray_client_; }

const Aws::String& ClientAws::GetClientRegion() const { return client_region_; }

}  // namespace skyrise
