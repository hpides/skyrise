#include "cost_calculator.hpp"

#include <cmath>
#include <cstddef>

#include <aws/ec2/model/InstanceType.h>

#include "utils/unit_conversion.hpp"

namespace skyrise {

long double CostCalculator::CalculateCostLambda(const size_t compute_duration_ms,
                                                const size_t function_instance_size_mb,
                                                const bool is_provisioned_concurrency) const {
  const auto& pricing = pricing_.GetLambdaPricing();
  const long double price_gb_second =
      is_provisioned_concurrency ? pricing->price_provisioned_gb_second : pricing->price_gb_second;
  const long double duration_cost =
      price_gb_second * MBToGB(function_instance_size_mb) * (compute_duration_ms / 1000.0L);

  return duration_cost + pricing->price_request;
}

long double CostCalculator::CalculateCostLambdaProvisionedConcurrency(const size_t provisioning_duration_ms,
                                                                      const size_t function_instance_size_mb,
                                                                      const size_t concurrent_instance_count) const {
  const auto& pricing = pricing_.GetLambdaPricing();
  const long double cost = pricing->price_provisioned_concurrency_gb_second * MBToGB(function_instance_size_mb) *
                           concurrent_instance_count * (provisioning_duration_ms / 1000.0L);

  return cost;
}

long double CostCalculator::CalculateCostLambdaProvisionedConcurrencyRounded(
    const size_t provisioning_duration_ms, const size_t function_instance_size_mb,
    const size_t concurrent_instance_count) const {
  const auto& pricing = pricing_.GetLambdaPricing();
  const auto pricing_intervals = static_cast<size_t>(std::ceil(provisioning_duration_ms / 1000.0L / 60.0L / 5.0L));
  const size_t duration_seconds = pricing_intervals * 5 * 60;
  const long double cost = pricing->price_provisioned_concurrency_gb_second * MBToGB(function_instance_size_mb) *
                           concurrent_instance_count * duration_seconds;

  return cost;
}

long double CostCalculator::CalculateCostS3StorageMonthly(const size_t used_storage_bytes, const size_t hours) const {
  const auto& pricing = pricing_.GetS3Pricing();
  const long double gb_months = ByteToGB(used_storage_bytes) * (hours / 24.0L / 30.0L);
  const long double storage_cost = pricing->price_storage_gb_months * gb_months;

  return storage_cost;
}

long double CostCalculator::CalculateCostS3Requests(const size_t requests_tier1, const size_t requests_tier2) const {
  const auto& pricing = pricing_.GetS3Pricing();
  const long double requests_tier1_cost = requests_tier1 * pricing->price_request_tier1;
  const long double requests_tier2_cost = requests_tier2 * pricing->price_request_tier2;

  return requests_tier1_cost + requests_tier2_cost;
}

long double CostCalculator::CalculateCostS3Select(const size_t returned_bytes, const size_t scanned_bytes) const {
  const auto& pricing = pricing_.GetS3Pricing();
  const long double returned_bytes_cost = ByteToGB(returned_bytes) * pricing->price_returned_gb_select;
  const long double scanned_bytes_cost = ByteToGB(scanned_bytes) * pricing->price_scanned_gb_select;

  return returned_bytes_cost + scanned_bytes_cost;
}

double CostCalculator::RoundTo(double value, double precision) { return std::round(value / precision) * precision; }

long double CostCalculator::CalculateCostXray(const size_t stored_functions, const size_t scanned_functions,
                                              const size_t accessed_functions) const {
  const auto& pricing = pricing_.GetXrayPricing();
  const long double stored_traces_price = pricing->price_per_stored_trace * stored_functions;
  const long double scanned_traces_price = pricing->price_per_accessed_trace * scanned_functions;
  const long double accessed_traces_price = pricing->price_per_accessed_trace * accessed_functions;

  return stored_traces_price + scanned_traces_price + accessed_traces_price;
}

long double CostCalculator::CalculateCostEc2(const size_t compute_duration_ms,
                                             const Aws::EC2::Model::InstanceType ec2_instance_type) const {
  const auto& pricing = pricing_.GetEc2Pricing(ec2_instance_type);
  const long double price_per_minute = pricing->price_per_hour / 60;  // Fixed price.
  const long double price_per_second = price_per_minute / 60;

  return compute_duration_ms <= 60000 ? price_per_minute
                                      : price_per_second * std::ceil(((long double)compute_duration_ms / 1000.0L));
}

}  // namespace skyrise
