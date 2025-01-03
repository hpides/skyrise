#pragma once

#include <aws/ec2/model/InstanceType.h>

#include "pricing.hpp"

namespace skyrise {

/*
 * The CostCalculator class uses service consumption information together with pricing information from the Pricing
 * class to estimate costs for the AWS services that Skyrise is built on. In order to keep costs comparable, it does not
 * take free tiers or any discounts into account.
 */

class CostCalculator {
 public:
  CostCalculator(std::shared_ptr<const Aws::Pricing::PricingClient> pricing_client, const std::string& client_region)
      : pricing_(std::move(pricing_client), client_region) {}

  // AWS rounds up the compute duration to the nearest 100ms
  long double CalculateCostEc2(const size_t compute_duration_ms,
                               const Aws::EC2::Model::InstanceType ec2_instance_type) const;
  long double CalculateCostLambda(const size_t compute_duration_ms, const size_t function_instance_size_mb,
                                  const bool is_provisioned_concurrency = false) const;
  long double CalculateCostLambdaProvisionedConcurrency(const size_t provisioning_duration_ms,
                                                        const size_t function_instance_size_mb,
                                                        const size_t concurrent_instance_count) const;
  long double CalculateCostLambdaProvisionedConcurrencyRounded(const size_t provisioning_duration_ms,
                                                               const size_t function_instance_size_mb,
                                                               const size_t concurrent_instance_count) const;
  long double CalculateCostXray(const size_t stored_functions, const size_t scanned_functions,
                                const size_t accessed_functions) const;

  /*
   * Any storage capacity that is being used on S3 is billed for at least a whole month - even if it's only stored for a
   * minute (cf. https://forums.aws.amazon.com/thread.jspa?threadID=118983). At this point, we only use the first
   * storage pricing unit (0-50TB) to guarantee comparability between calculations.
   */
  long double CalculateCostS3StorageMonthly(const size_t used_storage_bytes, const size_t hours) const;
  long double CalculateCostS3Requests(const size_t requests_tier1, const size_t requests_tier2) const;
  long double CalculateCostS3Select(const size_t returned_bytes, const size_t scanned_bytes) const;

  static double RoundTo(double value, double precision = 1.0);

 private:
  Pricing pricing_;
};

}  // namespace skyrise
