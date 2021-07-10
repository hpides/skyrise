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

class Client {
 public:
  Client();
  Client(const Client&) = delete;
  const Client& operator=(const Client&) = delete;

  std::shared_ptr<const Aws::CloudWatch::CloudWatchClient> GetCloudWatchClient() const;
  std::shared_ptr<const Aws::IAM::IAMClient> GetIAMClient() const;
  std::shared_ptr<const Aws::Lambda::LambdaClient> GetLambdaClient() const;
  std::shared_ptr<const Aws::Pricing::PricingClient> GetPricingClient() const;
  std::shared_ptr<const Aws::S3::S3Client> GetS3Client() const;
  std::shared_ptr<const Aws::SQS::SQSClient> GetSQSClient() const;
  std::shared_ptr<const Aws::XRay::XRayClient> GetXRayClient() const;

  const Aws::String& GetClientRegion() const;

 private:
  static Aws::Client::ClientConfiguration GenerateClientConfig();

  std::shared_ptr<const Aws::CloudWatch::CloudWatchClient> cloudwatch_client_;
  std::shared_ptr<const Aws::IAM::IAMClient> iam_client_;
  std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client_;
  std::shared_ptr<const Aws::Pricing::PricingClient> pricing_client_;
  std::shared_ptr<const Aws::S3::S3Client> s3_client_;
  std::shared_ptr<const Aws::SQS::SQSClient> sqs_client_;
  std::shared_ptr<const Aws::XRay::XRayClient> xray_client_;

  Aws::String client_region_;

  // Default location of certificate authority file on Amazon Linux 2
  inline static const Aws::String kCaFile{"/etc/pki/tls/certs/ca-bundle.crt"};
  static constexpr size_t kConnectTimeoutMs = 10'000;
  static constexpr bool kEnableTcpKeepAlive = false;
  static constexpr size_t kMaxConnections = 20'000;
  inline static Aws::String kPricingEndpoint = Aws::Region::US_EAST_1;
  static constexpr size_t kRequestTimeoutMs = 900'000;
  // TODO(anyone): Base the pool size on the underlying number of cores available
  static constexpr size_t kS3ExecutorPoolSize = 32;
};

}  // namespace skyrise
