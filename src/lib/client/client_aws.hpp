#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/Region.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/iam/IAMClient.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/monitoring/CloudWatchClient.h>
#include <aws/pricing/PricingClient.h>
#include <aws/s3/S3Client.h>
#include <aws/sqs/SQSClient.h>
#include <aws/xray/XRayClient.h>

namespace skyrise {

class ClientAws {
 public:
  ClientAws();
  ClientAws(const ClientAws&) = delete;
  const ClientAws& operator=(const ClientAws&) = delete;

  const Aws::CloudWatch::CloudWatchClient& GetCloudWatchClient() const;
  const Aws::IAM::IAMClient& GetIAMClient() const;
  const Aws::Lambda::LambdaClient& GetLambdaClient() const;
  const Aws::Pricing::PricingClient& GetPricingClient() const;
  const Aws::S3::S3Client& GetS3Client() const;
  const Aws::SQS::SQSClient& GetSQSClient() const;
  const Aws::XRay::XRayClient& GetXRayClient() const;

  const Aws::String& GetClientRegion() const;

 private:
  Aws::Client::ClientConfiguration GenerateClientConfig() const;

  std::unique_ptr<Aws::CloudWatch::CloudWatchClient> cloudwatch_client_;
  std::unique_ptr<Aws::IAM::IAMClient> iam_client_;
  std::unique_ptr<Aws::Lambda::LambdaClient> lambda_client_;
  std::unique_ptr<Aws::Pricing::PricingClient> pricing_client_;
  std::unique_ptr<Aws::S3::S3Client> s3_client_;
  std::unique_ptr<Aws::SQS::SQSClient> sqs_client_;
  std::unique_ptr<Aws::XRay::XRayClient> xray_client_;

  Aws::String client_region_;

  // Default location of certificate authority file on Amazon Linux 2
  const Aws::String kCaFile = "/etc/pki/tls/certs/ca-bundle.crt";
  const size_t kConnectTimeoutMs = 10'000;
  const bool kEnableTcpKeepAlive = false;
  const size_t kMaxConnections = 20'000;
  const Aws::String kPricingEndpoint = Aws::Region::US_EAST_1;
  const size_t kRequestTimeoutMs = 900'000;
};

}  // namespace skyrise
