#include "cost_calculator.hpp"

#include "utils/unit_conversion.hpp"

namespace skyrise {

long double CostCalculator::CalculateCostLambda(const size_t compute_duration_ms, const size_t lambda_size_mb) const {
  const auto pricing = pricing_->GetLambdaPricing();
  const size_t rounded_duration = (compute_duration_ms + 99) / 100 * 100;
  const long double duration_cost =
      pricing->price_per_gb_second * ByteToGb(MbToByte(lambda_size_mb)) * (rounded_duration / 1000.0L);

  return duration_cost + pricing->price_per_request;
}

long double CostCalculator::CalculateCostS3StorageMonthly(const size_t used_storage_bytes) const {
  const auto pricing = pricing_->GetS3Pricing();
  const long double storage_gb = ByteToGb(used_storage_bytes);
  const long double storage_cost = pricing->monthly_price_per_stored_gb * storage_gb;

  return storage_cost;
}

long double CostCalculator::CalculateCostS3Requests(const size_t requests_tier1, const size_t requests_tier2) const {
  const auto pricing = pricing_->GetS3Pricing();
  const long double requests_tier1_cost = requests_tier1 * pricing->price_per_request_tier1;
  const long double requests_tier2_cost = requests_tier2 * pricing->price_per_request_tier2;

  return requests_tier1_cost + requests_tier2_cost;
}

long double CostCalculator::CalculateCostS3Select(const size_t returned_bytes, const size_t scanned_bytes) const {
  const auto pricing = pricing_->GetS3Pricing();
  const long double returned_bytes_cost = ByteToGb(returned_bytes) * pricing->price_per_returned_gb_select;
  const long double scanned_bytes_cost = ByteToGb(scanned_bytes) * pricing->price_per_scanned_gb_select;

  return returned_bytes_cost + scanned_bytes_cost;
}

}  // namespace skyrise
