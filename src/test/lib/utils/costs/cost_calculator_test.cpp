#include "utils/costs/cost_calculator.hpp"

#include <functional>
#include <memory>

#include <aws/core/Region.h>

#include "costs_test_utils.hpp"
#include "gtest/gtest.h"
#include "utils/costs/pricing.hpp"

namespace skyrise {

class CostCalculatorTest : public ::testing::Test {};

TEST_F(CostCalculatorTest, CalculateCostLambda) {
  const std::function<void()> func = []() {
    const auto pricing = std::make_shared<Pricing>(Aws::Region::US_EAST_1);
    const CostCalculator cost_calculator(pricing);
    const auto lambda_pricing = pricing->GetLambdaPricing();

    const long double lambda_cost1 = cost_calculator.CalculateCostLambda(998, 512);
    const long double expected_cost1 = lambda_pricing->price_per_gb_second / 2.0L + lambda_pricing->price_per_request;

    EXPECT_EQ(lambda_cost1, expected_cost1);

    const long double lambda_cost2 = cost_calculator.CalculateCostLambda(30, 128);
    const long double expected_cost2 =
        lambda_pricing->price_per_gb_second / 10.0L / 8.0L + lambda_pricing->price_per_request;

    EXPECT_EQ(lambda_cost2, expected_cost2);

    const long double lambda_cost3 = cost_calculator.CalculateCostLambda(440, 256);
    const long double expected_cost3 =
        lambda_pricing->price_per_gb_second / 2.0L / 4.0L + lambda_pricing->price_per_request;

    EXPECT_EQ(lambda_cost3, expected_cost3);
  };

  InitAndShutDownAPI(func);
}

TEST_F(CostCalculatorTest, CalculateCostS3Storage) {
  const std::function<void()> func = []() {
    const auto pricing = std::make_shared<Pricing>(Aws::Region::US_EAST_1);
    const CostCalculator cost_calculator(pricing);
    const auto s3_pricing = pricing->GetS3Pricing();

    const long double storage_cost1 = cost_calculator.CalculateCostS3StorageMonthly(1073741823);
    const long double expected_cost1 = (1073741823.0 / 1073741824.0) * s3_pricing->monthly_price_per_stored_gb;
    EXPECT_EQ(storage_cost1, expected_cost1);

    const long double storage_cost2 = cost_calculator.CalculateCostS3StorageMonthly(1000);
    const long double expected_cost2 = (1000 / 1073741824.0) * s3_pricing->monthly_price_per_stored_gb;
    EXPECT_EQ(storage_cost2, expected_cost2);
  };

  InitAndShutDownAPI(func);
}

TEST_F(CostCalculatorTest, CalculateCostS3Requests) {
  const std::function<void()> func = []() {
    const auto pricing = std::make_shared<Pricing>(Aws::Region::US_EAST_1);
    const CostCalculator cost_calculator(pricing);
    const auto s3_pricing = pricing->GetS3Pricing();

    const long double requests_cost = cost_calculator.CalculateCostS3Requests(700, 800);
    const long double expected_cost =
        700 * s3_pricing->price_per_request_tier1 + 800 * s3_pricing->price_per_request_tier2;
    EXPECT_EQ(requests_cost, expected_cost);
  };

  InitAndShutDownAPI(func);
}

TEST_F(CostCalculatorTest, CalculateCostS3Select) {
  const std::function<void()> func = []() {
    const auto pricing = std::make_shared<Pricing>(Aws::Region::US_EAST_1);
    const CostCalculator cost_calculator(pricing);
    const auto s3_pricing = pricing->GetS3Pricing();

    const long double select_cost1 = cost_calculator.CalculateCostS3Select(1048576, 1048576);
    const long double expected_cost1 =
        s3_pricing->price_per_returned_gb_select / 1024.0 + s3_pricing->price_per_scanned_gb_select / 1024.0;
    EXPECT_EQ(select_cost1, expected_cost1);

    const long double select_cost2 = cost_calculator.CalculateCostS3Select(512, 134217728);
    const long double expected_cost2 = s3_pricing->price_per_returned_gb_select / 1024.0 / 1024.0 / 2.0 +
                                       s3_pricing->price_per_scanned_gb_select / 8.0;
    EXPECT_EQ(select_cost2, expected_cost2);
  };

  InitAndShutDownAPI(func);
}

}  // namespace skyrise
