#include "utils/costs/cost_calculator.hpp"

#include <functional>
#include <memory>

#include <aws/core/Region.h>
#include <gtest/gtest.h>

#include "client/client.hpp"
#include "testing/aws_test.hpp"
#include "utils/costs/pricing.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

class AwsCostCalculatorTest : public ::testing::Test {
 protected:
  const AwsApi aws_api_;
};

TEST_F(AwsCostCalculatorTest, CalculateCostLambda) {
  Client client;
  Pricing pricing(client.GetPricingClient(), client.GetClientRegion());
  const CostCalculator cost_calculator(client.GetPricingClient(), client.GetClientRegion());

  const auto& lambda_pricing = pricing.GetLambdaPricing();

  const long double lambda_cost1 = cost_calculator.CalculateCostLambda(998, 512);
  const long double expected_cost1 = lambda_pricing->price_gb_second_ * 0.5L * 0.998L + lambda_pricing->price_request_;

  EXPECT_EQ(lambda_cost1, expected_cost1);

  const long double lambda_cost2 = cost_calculator.CalculateCostLambda(30, 128, false);
  const long double expected_cost2 =
      lambda_pricing->price_gb_second_ * 0.125L * 0.030L + lambda_pricing->price_request_;

  EXPECT_EQ(lambda_cost2, expected_cost2);

  const long double lambda_cost3 = cost_calculator.CalculateCostLambda(440, 256, true);
  const long double expected_cost3 =
      lambda_pricing->price_provisioned_gb_second_ * 0.25L * 0.440L + lambda_pricing->price_request_;

  EXPECT_EQ(lambda_cost3, expected_cost3);

  const long double provisioned_concurrency_cost =
      cost_calculator.CalculateCostLambdaProvisionedConcurrency(60000, 1024, 100);
  const long double expected_provisioned_concurrency_cost =
      lambda_pricing->price_provisioned_concurrency_gb_second_ * 60.0L * 100.0L;

  EXPECT_EQ(provisioned_concurrency_cost, expected_provisioned_concurrency_cost);

  const long double provisioned_concurrency_cost_rounded =
      cost_calculator.CalculateCostLambdaProvisionedConcurrencyRounded(60000, 1024, 100);
  const long double expected_provisioned_concurrency_cost_rounded =
      lambda_pricing->price_provisioned_concurrency_gb_second_ * 300.0L * 100.0L;

  EXPECT_EQ(provisioned_concurrency_cost_rounded, expected_provisioned_concurrency_cost_rounded);
}

TEST_F(AwsCostCalculatorTest, CalculateCostS3StorageMonthly) {
  Client client;
  Pricing pricing(client.GetPricingClient(), client.GetClientRegion());
  const CostCalculator cost_calculator(client.GetPricingClient(), client.GetClientRegion());

  const auto& s3_pricing = pricing.GetS3Pricing();

  const long double storage_cost1 = cost_calculator.CalculateCostS3StorageMonthly(MbToByte(1023), 1);
  const long double expected_cost1 =
      ByteToGb(MbToByte(1023)) * 1 / 24.0L / 30.0L * s3_pricing->price_storage_gb_months_;
  EXPECT_EQ(storage_cost1, expected_cost1);

  const long double storage_cost2 = cost_calculator.CalculateCostS3StorageMonthly(KbToByte(15), 72);
  const long double expected_cost2 = ByteToGb(KbToByte(15)) * 72 / 24.0L / 30.0L * s3_pricing->price_storage_gb_months_;
  EXPECT_EQ(storage_cost2, expected_cost2);
}

TEST_F(AwsCostCalculatorTest, CalculateCostS3Requests) {
  Client client;
  Pricing pricing(client.GetPricingClient(), client.GetClientRegion());
  const CostCalculator cost_calculator(client.GetPricingClient(), client.GetClientRegion());

  const auto& s3_pricing = pricing.GetS3Pricing();

  const long double requests_cost = cost_calculator.CalculateCostS3Requests(700, 800);
  const long double expected_cost = 700 * s3_pricing->price_request_tier1_ + 800 * s3_pricing->price_request_tier2_;
  EXPECT_EQ(requests_cost, expected_cost);
}

TEST_F(AwsCostCalculatorTest, CalculateCostS3Select) {
  Client client;
  Pricing pricing(client.GetPricingClient(), client.GetClientRegion());
  const CostCalculator cost_calculator(client.GetPricingClient(), client.GetClientRegion());

  const auto& s3_pricing = pricing.GetS3Pricing();

  const long double select_cost1 = cost_calculator.CalculateCostS3Select(1048576, 1048576);
  const long double expected_cost1 =
      s3_pricing->price_returned_gb_select_ / 1024.0 + s3_pricing->price_scanned_gb_select_ / 1024.0;
  EXPECT_EQ(select_cost1, expected_cost1);

  const long double select_cost2 = cost_calculator.CalculateCostS3Select(512, 134217728);
  const long double expected_cost2 =
      s3_pricing->price_returned_gb_select_ / 1024.0 / 1024.0 / 2.0 + s3_pricing->price_scanned_gb_select_ / 8.0;
  EXPECT_EQ(select_cost2, expected_cost2);
}

TEST_F(AwsCostCalculatorTest, CalculateCostXray) {
  Client client;
  Pricing pricing(client.GetPricingClient(), client.GetClientRegion());
  const CostCalculator cost_calculator(client.GetPricingClient(), client.GetClientRegion());

  const auto& xray_pricing = pricing.GetXrayPricing();

  const long double traces_cost = cost_calculator.CalculateCostXray(1000, 10000, 1000);
  const long double expected_cost =
      1000 * xray_pricing->price_per_stored_trace + 11000 * xray_pricing->price_per_accessed_trace;

  EXPECT_EQ(traces_cost, expected_cost);
}

}  // namespace skyrise
