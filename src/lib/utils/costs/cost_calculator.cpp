#include "cost_calculator.hpp"

#include <cmath>

#include "utils/unit_conversion.hpp"

namespace skyrise {

long double CostCalculator::CalculateCostLambda(const size_t compute_ms_duration,
                                                const size_t function_instance_mb_size,
                                                const bool is_provisioned_concurrency) const {
  const auto& pricing = pricing_.GetLambdaPricing();
  const long double price_gb_second =
      is_provisioned_concurrency ? pricing->price_provisioned_gb_second_ : pricing->price_gb_second_;
  const long double duration_cost =
      price_gb_second * ByteToGb(MbToByte(function_instance_mb_size)) * (compute_ms_duration / 1000.0L);

  return duration_cost + pricing->price_request_;
}

long double CostCalculator::CalculateCostLambdaProvisionedConcurrency(const size_t provisioning_ms_duration,
                                                                      const size_t function_instance_mb_size,
                                                                      const size_t invocation_count) const {
  const auto& pricing = pricing_.GetLambdaPricing();
  const long double cost = pricing->price_provisioned_concurrency_gb_second_ *
                           ByteToGb(MbToByte(function_instance_mb_size)) * invocation_count *
                           (provisioning_ms_duration / 1000.0L);

  return cost;
}

long double CostCalculator::CalculateCostLambdaProvisionedConcurrencyRounded(const size_t provisioning_ms_duration,
                                                                             const size_t function_instance_mb_size,
                                                                             const size_t invocation_count) const {
  const auto& pricing = pricing_.GetLambdaPricing();
  const auto pricing_intervals = static_cast<size_t>(std::ceil(provisioning_ms_duration / 1000.0L / 60.0L / 5.0L));
  const size_t duration_seconds = pricing_intervals * 5 * 60;
  const long double cost = pricing->price_provisioned_concurrency_gb_second_ *
                           ByteToGb(MbToByte(function_instance_mb_size)) * invocation_count * duration_seconds;

  return cost;
}

long double CostCalculator::CalculateCostS3StorageMonthly(const size_t used_storage_bytes, const size_t hours) const {
  const auto& pricing = pricing_.GetS3Pricing();
  const long double gb_months = ByteToGb(used_storage_bytes) * (hours / 24.0L / 30.0L);
  const long double storage_cost = pricing->price_storage_gb_months_ * gb_months;

  return storage_cost;
}

long double CostCalculator::CalculateCostS3Requests(const size_t requests_tier1, const size_t requests_tier2) const {
  const auto& pricing = pricing_.GetS3Pricing();
  const long double requests_tier1_cost = requests_tier1 * pricing->price_request_tier1_;
  const long double requests_tier2_cost = requests_tier2 * pricing->price_request_tier2_;

  return requests_tier1_cost + requests_tier2_cost;
}

long double CostCalculator::CalculateCostS3Select(const size_t returned_bytes, const size_t scanned_bytes) const {
  const auto& pricing = pricing_.GetS3Pricing();
  const long double returned_bytes_cost = ByteToGb(returned_bytes) * pricing->price_returned_gb_select_;
  const long double scanned_bytes_cost = ByteToGb(scanned_bytes) * pricing->price_scanned_gb_select_;

  return returned_bytes_cost + scanned_bytes_cost;
}

long double CostCalculator::CalculateCostXray(const size_t stored_functions, const size_t scanned_functions,
                                              const size_t accessed_functions) const {
  const auto& pricing = pricing_.GetXrayPricing();
  const long double stored_traces_price = pricing->price_per_stored_trace * stored_functions;
  const long double scanned_traces_price = pricing->price_per_accessed_trace * scanned_functions;
  const long double accessed_traces_price = pricing->price_per_accessed_trace * accessed_functions;

  return stored_traces_price + scanned_traces_price + accessed_traces_price;
}

}  // namespace skyrise
