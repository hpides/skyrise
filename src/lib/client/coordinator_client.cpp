#include "coordinator_client.hpp"

#include <future>

#include "function/function_utils.hpp"
#include "utils/assert.hpp"
#include "utils/profiling/function_host_information.hpp"

namespace skyrise {

CoordinatorClient::CoordinatorClient() {
  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();
  if (!credentials_provider || (*credentials_provider).GetAWSCredentials().IsExpiredOrEmpty()) {
    Fail("AWS credentials are missing or expired. Please export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.\n");
  }

  auto client_configuration = GenerateClientConfig();

  // The Pricing API does not have endpoints in every region and is therefore always initialized with us-east-1.
  auto client_configuration_pricing = GenerateClientConfig();
  client_configuration_pricing.region = kPricingEndpoint;

  const std::vector<std::function<void()>> initializers{
      [&]() {
        cloudwatch_client_ =
            std::make_shared<const Aws::CloudWatch::CloudWatchClient>(credentials_provider, client_configuration);
      },
      [&]() { ec2_client_ = std::make_shared<const Aws::EC2::EC2Client>(credentials_provider, client_configuration); },
      [&]() {
        glue_client_ = std::make_shared<const Aws::Glue::GlueClient>(credentials_provider, client_configuration);
      },
      [&]() { iam_client_ = std::make_shared<const Aws::IAM::IAMClient>(credentials_provider, client_configuration); },
      [&]() {
        pricing_client_ =
            std::make_shared<const Aws::Pricing::PricingClient>(credentials_provider, client_configuration_pricing);
      },
      [&]() { ssm_client_ = std::make_shared<const Aws::SSM::SSMClient>(credentials_provider, client_configuration); },
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

std::shared_ptr<const Aws::CloudWatch::CloudWatchClient> CoordinatorClient::GetCloudWatchClient() const {
  return cloudwatch_client_;
}

std::shared_ptr<const Aws::EC2::EC2Client> CoordinatorClient::GetEc2Client() const { return ec2_client_; }

std::shared_ptr<const Aws::Glue::GlueClient> CoordinatorClient::GetGlueClient() const { return glue_client_; }

std::shared_ptr<const Aws::IAM::IAMClient> CoordinatorClient::GetIamClient() const { return iam_client_; }

std::shared_ptr<const Aws::Pricing::PricingClient> CoordinatorClient::GetPricingClient() const {
  return pricing_client_;
}

std::shared_ptr<const Aws::SSM::SSMClient> CoordinatorClient::GetSsmClient() const { return ssm_client_; }

std::shared_ptr<const Aws::XRay::XRayClient> CoordinatorClient::GetXRayClient() const { return xray_client_; }

}  // namespace skyrise
