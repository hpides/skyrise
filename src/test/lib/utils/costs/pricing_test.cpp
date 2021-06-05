#include "utils/costs/pricing.hpp"

#include <functional>

#include <aws/core/Region.h>
#include <gtest/gtest.h>

#include "client/client.hpp"
#include "costs_test_utils.hpp"

namespace skyrise {

class AwsPricingTest : public ::testing::Test {};

TEST_F(AwsPricingTest, PricingLambda) {
  const std::function<void()> func = []() {
    const auto clients = std::make_shared<Client>();
    Pricing pricing(clients);

    const auto& lambda_pricing1 = pricing.GetLambdaPricing();

    EXPECT_GT(lambda_pricing1->price_request_, 0);
    EXPECT_GT(lambda_pricing1->price_gb_second_, 0);
    EXPECT_GT(lambda_pricing1->price_provisioned_gb_second_, 0);
    EXPECT_GT(lambda_pricing1->price_provisioned_concurrency_gb_second_, 0);

    const auto& lambda_pricing2 = pricing.GetLambdaPricing();

    EXPECT_EQ(lambda_pricing1->price_request_, lambda_pricing2->price_request_);
    EXPECT_EQ(lambda_pricing1->price_gb_second_, lambda_pricing2->price_gb_second_);
    EXPECT_EQ(lambda_pricing1->price_provisioned_gb_second_, lambda_pricing2->price_provisioned_gb_second_);
    EXPECT_EQ(lambda_pricing1->price_provisioned_concurrency_gb_second_,
              lambda_pricing2->price_provisioned_concurrency_gb_second_);
  };

  InitAndShutDownAPI(func);
}

TEST_F(AwsPricingTest, PricingS3) {
  const std::function<void()> func = []() {
    const auto clients = std::make_shared<Client>();
    Pricing pricing(clients);

    const auto& s3_pricing1 = pricing.GetS3Pricing();

    EXPECT_GT(s3_pricing1->price_request_tier1_, 0);
    EXPECT_GT(s3_pricing1->price_request_tier2_, 0);
    EXPECT_GT(s3_pricing1->price_storage_gb_months_, 0);
    EXPECT_GT(s3_pricing1->price_returned_gb_select_, 0);
    EXPECT_GT(s3_pricing1->price_scanned_gb_select_, 0);
    EXPECT_GT(s3_pricing1->price_storage_tag_hours_, 0);

    const auto& s3_pricing2 = pricing.GetS3Pricing();

    EXPECT_EQ(s3_pricing1->price_request_tier1_, s3_pricing2->price_request_tier1_);
    EXPECT_EQ(s3_pricing1->price_request_tier2_, s3_pricing2->price_request_tier2_);
    EXPECT_EQ(s3_pricing1->price_storage_gb_months_, s3_pricing2->price_storage_gb_months_);
    EXPECT_EQ(s3_pricing1->price_returned_gb_select_, s3_pricing2->price_returned_gb_select_);
    EXPECT_EQ(s3_pricing1->price_scanned_gb_select_, s3_pricing2->price_scanned_gb_select_);
    EXPECT_EQ(s3_pricing1->price_storage_tag_hours_, s3_pricing2->price_storage_tag_hours_);
  };

  InitAndShutDownAPI(func);
}

TEST_F(AwsPricingTest, PricingXray) {
  const std::function<void()> func = []() {
    const auto clients = std::make_shared<Client>();
    Pricing pricing(clients);

    const auto xray_pricing_1 = pricing.GetXrayPricing();

    EXPECT_GT(xray_pricing_1->price_per_stored_trace, 0);
    EXPECT_GT(xray_pricing_1->price_per_accessed_trace, 0);

    const auto xray_pricing_2 = pricing.GetXrayPricing();

    EXPECT_EQ(xray_pricing_1->price_per_stored_trace, xray_pricing_2->price_per_stored_trace);
    EXPECT_EQ(xray_pricing_1->price_per_accessed_trace, xray_pricing_2->price_per_accessed_trace);
  };

  InitAndShutDownAPI(func);
}

}  // namespace skyrise
