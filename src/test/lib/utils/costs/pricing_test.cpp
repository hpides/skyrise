#include "utils/costs/pricing.hpp"

#include <functional>

#include <aws/core/Region.h>
#include <gtest/gtest.h>

#include "client/coordinator_client.hpp"
#include "testing/aws_test.hpp"

namespace skyrise {

class AwsPricingTest : public testing::TestWithParam<std::pair<double, Aws::EC2::Model::InstanceType>> {
 private:
  const AwsApi aws_api_;

 protected:
  const CoordinatorClient client_;
};

// The prices are subject to change, which may lead to failing tests.
// If so, the prices should be updated according to https://aws.amazon.com/ec2/pricing/on-demand/.
INSTANTIATE_TEST_SUITE_P(AwsEc2PricingTest, AwsPricingTest,
                         testing::Values(std::make_pair(2.7648, Aws::EC2::Model::InstanceType::c6gn_16xlarge),
                                         std::make_pair(0.0385, Aws::EC2::Model::InstanceType::m6g_medium),
                                         std::make_pair(0.0042, Aws::EC2::Model::InstanceType::t4g_nano)));

TEST_F(AwsPricingTest, PricingLambda) {
  const Pricing pricing(client_.GetPricingClient(), client_.GetClientRegion());

  const auto& lambda_pricing1 = pricing.GetLambdaPricing();

  EXPECT_GT(lambda_pricing1->price_request, 0);
  EXPECT_GT(lambda_pricing1->price_gb_second, 0);
  EXPECT_GT(lambda_pricing1->price_provisioned_gb_second, 0);
  EXPECT_GT(lambda_pricing1->price_provisioned_concurrency_gb_second, 0);

  const auto& lambda_pricing2 = pricing.GetLambdaPricing();

  EXPECT_EQ(lambda_pricing1->price_request, lambda_pricing2->price_request);
  EXPECT_EQ(lambda_pricing1->price_gb_second, lambda_pricing2->price_gb_second);
  EXPECT_EQ(lambda_pricing1->price_provisioned_gb_second, lambda_pricing2->price_provisioned_gb_second);
  EXPECT_EQ(lambda_pricing1->price_provisioned_concurrency_gb_second,
            lambda_pricing2->price_provisioned_concurrency_gb_second);
}

TEST_F(AwsPricingTest, PricingS3) {
  const Pricing pricing(client_.GetPricingClient(), client_.GetClientRegion());

  const auto& s3_pricing1 = pricing.GetS3Pricing();

  EXPECT_GT(s3_pricing1->price_request_tier1, 0);
  EXPECT_GT(s3_pricing1->price_request_tier2, 0);
  EXPECT_GT(s3_pricing1->price_storage_gb_months, 0);
  EXPECT_GT(s3_pricing1->price_returned_gb_select, 0);
  EXPECT_GT(s3_pricing1->price_scanned_gb_select, 0);
  EXPECT_GT(s3_pricing1->price_storage_tag_hours, 0);

  const auto& s3_pricing2 = pricing.GetS3Pricing();

  EXPECT_EQ(s3_pricing1->price_request_tier1, s3_pricing2->price_request_tier1);
  EXPECT_EQ(s3_pricing1->price_request_tier2, s3_pricing2->price_request_tier2);
  EXPECT_EQ(s3_pricing1->price_storage_gb_months, s3_pricing2->price_storage_gb_months);
  EXPECT_EQ(s3_pricing1->price_returned_gb_select, s3_pricing2->price_returned_gb_select);
  EXPECT_EQ(s3_pricing1->price_scanned_gb_select, s3_pricing2->price_scanned_gb_select);
  EXPECT_EQ(s3_pricing1->price_storage_tag_hours, s3_pricing2->price_storage_tag_hours);
}

TEST_F(AwsPricingTest, PricingXray) {
  const Pricing pricing(client_.GetPricingClient(), client_.GetClientRegion());

  const auto& xray_pricing_1 = pricing.GetXrayPricing();

  EXPECT_GT(xray_pricing_1->price_per_stored_trace, 0);
  EXPECT_GT(xray_pricing_1->price_per_accessed_trace, 0);

  const auto& xray_pricing_2 = pricing.GetXrayPricing();

  EXPECT_EQ(xray_pricing_1->price_per_stored_trace, xray_pricing_2->price_per_stored_trace);
  EXPECT_EQ(xray_pricing_1->price_per_accessed_trace, xray_pricing_2->price_per_accessed_trace);
}

TEST_F(AwsPricingTest, DifferentPricingRegions) {
  const std::vector<std::string> pricing_regions = {Aws::Region::US_EAST_2, Aws::Region::US_WEST_1,
                                                    Aws::Region::EU_CENTRAL_1, Aws::Region::EU_WEST_1,
                                                    Aws::Region::AF_SOUTH_1};

  for (const auto& pricing_region : pricing_regions) {
    const Pricing pricing(client_.GetPricingClient(), pricing_region);
    EXPECT_NE(pricing.GetLambdaPricing(), nullptr);
    EXPECT_NE(pricing.GetS3Pricing(), nullptr);
    EXPECT_NE(pricing.GetXrayPricing(), nullptr);
  }
}

TEST_P(AwsPricingTest, PricingEc2) {
  const auto [price, instance_type] = GetParam();
  Pricing pricing(client_.GetPricingClient(), client_.GetClientRegion());
  const auto& ec2_pricing = pricing.GetEc2Pricing(instance_type);

  EXPECT_EQ(ec2_pricing->price_per_hour, price);
}

}  // namespace skyrise
