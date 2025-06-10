#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/Region.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/ec2/EC2Client.h>
#include <aws/glue/GlueClient.h>
#include <aws/iam/IAMClient.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/monitoring/CloudWatchClient.h>
#include <aws/pricing/PricingClient.h>
#include <aws/s3/S3Client.h>
#include <aws/sqs/SQSClient.h>
#include <aws/ssm/SSMClient.h>
#include <aws/xray/XRayClient.h>

#include "client/base_client.hpp"

namespace skyrise {

class CoordinatorClient : public BaseClient {
 public:
  CoordinatorClient();
  CoordinatorClient(const CoordinatorClient&) = delete;
  CoordinatorClient(CoordinatorClient&&) = default;
  const CoordinatorClient& operator=(const CoordinatorClient&) = delete;
  CoordinatorClient& operator=(CoordinatorClient&&) = default;

  ~CoordinatorClient() = default;

  std::shared_ptr<const Aws::CloudWatch::CloudWatchClient> GetCloudWatchClient() const;
  std::shared_ptr<const Aws::EC2::EC2Client> GetEc2Client() const;
  std::shared_ptr<const Aws::Glue::GlueClient> GetGlueClient() const;
  std::shared_ptr<const Aws::IAM::IAMClient> GetIamClient() const;
  std::shared_ptr<const Aws::Pricing::PricingClient> GetPricingClient() const;
  std::shared_ptr<const Aws::SSM::SSMClient> GetSsmClient() const;
  std::shared_ptr<const Aws::XRay::XRayClient> GetXRayClient() const;
  std::shared_ptr<Aws::S3::S3Client> GetMutableS3Client();

 private:
  std::shared_ptr<const Aws::CloudWatch::CloudWatchClient> cloudwatch_client_;
  std::shared_ptr<const Aws::EC2::EC2Client> ec2_client_;
  std::shared_ptr<const Aws::Glue::GlueClient> glue_client_;
  std::shared_ptr<const Aws::IAM::IAMClient> iam_client_;
  std::shared_ptr<const Aws::Pricing::PricingClient> pricing_client_;
  std::shared_ptr<const Aws::SSM::SSMClient> ssm_client_;
  std::shared_ptr<const Aws::XRay::XRayClient> xray_client_;
  std::shared_ptr<Aws::S3::S3Client> mutable_s3_client_;

  inline static const Aws::String kPricingEndpoint = Aws::Region::US_EAST_1;
};

}  // namespace skyrise
