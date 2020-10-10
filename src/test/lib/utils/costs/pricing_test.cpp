#include "utils/costs/pricing.hpp"

#include <functional>

#include <aws/core/Region.h>

#include "client/client_aws.hpp"
#include "costs_test_utils.hpp"
#include "gtest/gtest.h"

namespace skyrise {

class PricingTest : public ::testing::Test {};

TEST_F(PricingTest, PricingLambda) {
  const std::function<void()> func = []() {
    const auto clients = std::make_shared<ClientAws>();
    Pricing pricing(clients);

    const auto lambda_pricing1 = pricing.GetLambdaPricing();

    EXPECT_GT(lambda_pricing1->price_per_request, 0);
    EXPECT_GT(lambda_pricing1->price_per_gb_second, 0);
    EXPECT_GT(lambda_pricing1->price_per_provisioned_gb_second, 0);
    EXPECT_GT(lambda_pricing1->price_per_provisioned_concurrency_gb_second, 0);

    const auto lambda_pricing2 = pricing.GetLambdaPricing();

    EXPECT_EQ(lambda_pricing1->price_per_request, lambda_pricing2->price_per_request);
    EXPECT_EQ(lambda_pricing1->price_per_gb_second, lambda_pricing2->price_per_gb_second);
    EXPECT_EQ(lambda_pricing1->price_per_provisioned_gb_second, lambda_pricing2->price_per_provisioned_gb_second);
    EXPECT_EQ(lambda_pricing1->price_per_provisioned_concurrency_gb_second,
              lambda_pricing2->price_per_provisioned_concurrency_gb_second);
  };

  InitAndShutDownAPI(func);
}

TEST_F(PricingTest, PricingS3) {
  const std::function<void()> func = []() {
    const auto clients = std::make_shared<ClientAws>();
    Pricing pricing(clients);

    const auto s3_pricing1 = pricing.GetS3Pricing();

    EXPECT_GT(s3_pricing1->price_per_request_tier1, 0);
    EXPECT_GT(s3_pricing1->price_per_request_tier2, 0);
    EXPECT_GT(s3_pricing1->monthly_price_per_stored_gb, 0);
    EXPECT_GT(s3_pricing1->price_per_returned_gb_select, 0);
    EXPECT_GT(s3_pricing1->price_per_scanned_gb_select, 0);
    EXPECT_GT(s3_pricing1->monthly_price_per_tag, 0);

    const auto s3_pricing2 = pricing.GetS3Pricing();

    EXPECT_EQ(s3_pricing1->price_per_request_tier1, s3_pricing2->price_per_request_tier1);
    EXPECT_EQ(s3_pricing1->price_per_request_tier2, s3_pricing2->price_per_request_tier2);
    EXPECT_EQ(s3_pricing1->monthly_price_per_stored_gb, s3_pricing2->monthly_price_per_stored_gb);
    EXPECT_EQ(s3_pricing1->price_per_returned_gb_select, s3_pricing2->price_per_returned_gb_select);
    EXPECT_EQ(s3_pricing1->price_per_scanned_gb_select, s3_pricing2->price_per_scanned_gb_select);
    EXPECT_EQ(s3_pricing1->monthly_price_per_tag, s3_pricing2->monthly_price_per_tag);
  };

  InitAndShutDownAPI(func);
}

}  // namespace skyrise
