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
    const auto lambda_pricing = pricing->get_lambda_pricing();

    const auto lambda_cost1 = cost_calculator.calculate_cost_lambda(998, 512);
    const auto expected_cost1 = lambda_pricing->price_per_gb_second / 2.0 + lambda_pricing->price_per_request;

    EXPECT_EQ(lambda_cost1, expected_cost1);

    const auto lambda_cost2 = cost_calculator.calculate_cost_lambda(30, 128);
    const auto expected_cost2 = lambda_pricing->price_per_gb_second / 10.0 / 8.0 + lambda_pricing->price_per_request;

    EXPECT_EQ(lambda_cost2, expected_cost2);

    const auto lambda_cost3 = cost_calculator.calculate_cost_lambda(440, 256);
    const auto expected_cost3 = lambda_pricing->price_per_gb_second / 2.0 / 4.0 + lambda_pricing->price_per_request;

    EXPECT_EQ(lambda_cost3, expected_cost3);
  };

  InitAndShutDownAPI(func);
}

TEST_F(CostCalculatorTest, CalculateCostS3Storage) {
  const std::function<void()> func = []() {
    const auto pricing = std::make_shared<Pricing>(Aws::Region::US_EAST_1);
    const CostCalculator cost_calculator(pricing);
    const auto s3_pricing = pricing->get_s3_pricing();

    const auto storage_cost1 = cost_calculator.calculate_cost_s3_storage_monthly(1073741823);
    const auto expected_cost1 = (1073741823.0 / 1073741824.0) * s3_pricing->monthly_price_per_stored_gb;
    EXPECT_EQ(storage_cost1, expected_cost1);

    const auto storage_cost2 = cost_calculator.calculate_cost_s3_storage_monthly(1000);
    const auto expected_cost2 = (1000 / 1073741824.0) * s3_pricing->monthly_price_per_stored_gb;
    EXPECT_EQ(storage_cost2, expected_cost2);
  };

  InitAndShutDownAPI(func);
}

TEST_F(CostCalculatorTest, CalculateCostS3Requests) {
  const std::function<void()> func = []() {
    const auto pricing = std::make_shared<Pricing>(Aws::Region::US_EAST_1);
    const CostCalculator cost_calculator(pricing);
    const auto s3_pricing = pricing->get_s3_pricing();

    const auto requests_cost = cost_calculator.calculate_cost_s3_requests(700, 800);
    const auto expected_cost = 700 * s3_pricing->price_per_request_tier1 + 800 * s3_pricing->price_per_request_tier2;
    EXPECT_EQ(requests_cost, expected_cost);
  };

  InitAndShutDownAPI(func);
}

TEST_F(CostCalculatorTest, CalculateCostS3Select) {
  const std::function<void()> func = []() {
    const auto pricing = std::make_shared<Pricing>(Aws::Region::US_EAST_1);
    const CostCalculator cost_calculator(pricing);
    const auto s3_pricing = pricing->get_s3_pricing();

    const auto select_cost1 = cost_calculator.calculate_cost_s3_select(1048576, 1048576);
    const auto expected_cost1 =
        s3_pricing->price_per_returned_gb_select / 1024.0 + s3_pricing->price_per_scanned_gb_select / 1024.0;
    EXPECT_EQ(select_cost1, expected_cost1);

    const auto select_cost2 = cost_calculator.calculate_cost_s3_select(512, 134217728);
    const auto expected_cost2 = s3_pricing->price_per_returned_gb_select / 1024.0 / 1024.0 / 2.0 +
                                s3_pricing->price_per_scanned_gb_select / 8.0;
    EXPECT_EQ(select_cost2, expected_cost2);
  };

  InitAndShutDownAPI(func);
}

}  // namespace skyrise
