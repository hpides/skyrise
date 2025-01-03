#include "utils/costs/cost_calculator.hpp"

#include <functional>

#include <gtest/gtest.h>

#include "client/coordinator_client.hpp"
#include "testing/aws_test.hpp"
#include "utils/costs/pricing.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

class AwsCostCalculatorTest : public ::testing::Test {
 public:
  AwsCostCalculatorTest()
      : client_(),
        pricing_(client_.GetPricingClient(), client_.GetClientRegion()),
        cost_calculator_(client_.GetPricingClient(), client_.GetClientRegion()) {}

 protected:
  const AwsApi aws_api_;
  const CoordinatorClient client_;
  const Pricing pricing_;
  const CostCalculator cost_calculator_;
};

TEST_F(AwsCostCalculatorTest, CalculateCostLambda) {
  const auto& lambda_pricing = pricing_.GetLambdaPricing();

  const long double lambda_cost1 = cost_calculator_.CalculateCostLambda(998, 500);
  const long double expected_cost1 = lambda_pricing->price_gb_second * 0.5L * 0.998L + lambda_pricing->price_request;

  EXPECT_EQ(lambda_cost1, expected_cost1);

  const long double lambda_cost2 = cost_calculator_.CalculateCostLambda(30, 128, false);
  const long double expected_cost2 = lambda_pricing->price_gb_second * 0.128L * 0.030L + lambda_pricing->price_request;

  EXPECT_EQ(lambda_cost2, expected_cost2);

  const long double lambda_cost3 = cost_calculator_.CalculateCostLambda(440, 256, true);
  const long double expected_cost3 =
      lambda_pricing->price_provisioned_gb_second * 0.256L * 0.440L + lambda_pricing->price_request;

  EXPECT_EQ(lambda_cost3, expected_cost3);

  const long double provisioned_concurrency_cost =
      cost_calculator_.CalculateCostLambdaProvisionedConcurrency(60000, 1024, 100);
  const long double expected_provisioned_concurrency_cost =
      lambda_pricing->price_provisioned_concurrency_gb_second * 60.0L * 1.024L * 100.0L;

  EXPECT_EQ(provisioned_concurrency_cost, expected_provisioned_concurrency_cost);

  const long double provisioned_concurrency_cost_rounded =
      cost_calculator_.CalculateCostLambdaProvisionedConcurrencyRounded(60000, 1024, 100);
  const long double expected_provisioned_concurrency_cost_rounded =
      lambda_pricing->price_provisioned_concurrency_gb_second * 300L * 1.024L * 100L;

  EXPECT_EQ(CostCalculator::RoundTo(provisioned_concurrency_cost_rounded, 0.0000001L),
            CostCalculator::RoundTo(expected_provisioned_concurrency_cost_rounded, 0.0000001L));
}

TEST_F(AwsCostCalculatorTest, CalculateCostS3StorageMonthly) {
  const auto& s3_pricing = pricing_.GetS3Pricing();

  const long double storage_cost1 = cost_calculator_.CalculateCostS3StorageMonthly(MBToByte(1023), 1);
  const long double expected_cost1 = MBToGB(1023) * 1 / 24.0L / 30.0L * s3_pricing->price_storage_gb_months;
  EXPECT_EQ(storage_cost1, expected_cost1);

  const long double storage_cost2 = cost_calculator_.CalculateCostS3StorageMonthly(KBToByte(15), 72);
  const long double expected_cost2 = KBToGB(15) * 72 / 24.0L / 30.0L * s3_pricing->price_storage_gb_months;
  EXPECT_EQ(storage_cost2, expected_cost2);
}

TEST_F(AwsCostCalculatorTest, CalculateCostS3Requests) {
  const auto& s3_pricing = pricing_.GetS3Pricing();

  const long double requests_cost = cost_calculator_.CalculateCostS3Requests(700, 800);
  const long double expected_cost = 700 * s3_pricing->price_request_tier1 + 800 * s3_pricing->price_request_tier2;
  EXPECT_EQ(requests_cost, expected_cost);
}

TEST_F(AwsCostCalculatorTest, CalculateCostS3Select) {
  const auto& s3_pricing = pricing_.GetS3Pricing();

  const long double select_cost1 = cost_calculator_.CalculateCostS3Select(MBToByte(1), MBToByte(1));
  const long double expected_cost1 =
      s3_pricing->price_returned_gb_select / 1000.0 + s3_pricing->price_scanned_gb_select / 1000.0;
  EXPECT_EQ(select_cost1, expected_cost1);

  const long double select_cost2 = cost_calculator_.CalculateCostS3Select(500, MBToByte(125));
  const long double expected_cost2 =
      s3_pricing->price_returned_gb_select / 1000.0 / 1000.0 / 2.0 + s3_pricing->price_scanned_gb_select / 8.0;
  EXPECT_EQ(select_cost2, expected_cost2);
}

TEST_F(AwsCostCalculatorTest, CalculateCostXray) {
  const auto& xray_pricing = pricing_.GetXrayPricing();

  const long double traces_cost = cost_calculator_.CalculateCostXray(1000, 10000, 1000);
  const long double expected_cost =
      1000 * xray_pricing->price_per_stored_trace + 11000 * xray_pricing->price_per_accessed_trace;

  EXPECT_EQ(traces_cost, expected_cost);
}

TEST_F(AwsCostCalculatorTest, CalculateCostEc2) {
  Aws::EC2::Model::InstanceType instance_type = Aws::EC2::Model::InstanceType::c6gn_16xlarge;
  const size_t runtime = 80000;          // Total runtime of 80 seconds.
  const double fixed_cost = 0.04608;     // Costs for first minute.
  const double variable_cost = 0.01536;  // Costs for 20 seconds.

  const long double ec2_costs = cost_calculator_.CalculateCostEc2(runtime, instance_type);
  const long double expected_cost = fixed_cost + variable_cost;

  EXPECT_DOUBLE_EQ(ec2_costs, expected_cost);
}

TEST_F(AwsCostCalculatorTest, CalculateCostEc2Minimal) {
  Aws::EC2::Model::InstanceType instance_type = Aws::EC2::Model::InstanceType::c6gn_16xlarge;
  const size_t runtime = 10000;            // Total runtime of 10 seconds.
  const long double fixed_cost = 0.04608;  // Costs for first minute.

  const long double ec2_costs = cost_calculator_.CalculateCostEc2(runtime, instance_type);

  EXPECT_DOUBLE_EQ(ec2_costs, fixed_cost);
}
}  // namespace skyrise
