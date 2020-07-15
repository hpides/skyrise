#include "cost_calculator.hpp"

namespace skyrise {

double CostCalculator::calculate_cost_lambda(const size_t compute_duration_ms, const size_t lambda_size_mb) const {
  const auto pricing = _pricing->get_lambda_pricing();
  const size_t rounded_duration = (compute_duration_ms + 99) / 100 * 100;
  const double duration_cost = pricing->price_per_gb_second * (lambda_size_mb / 1024.0) * (rounded_duration / 1000.0);

  return duration_cost + pricing->price_per_request;
}

double CostCalculator::calculate_cost_s3_storage_monthly(const size_t used_storage_bytes) const {
  const auto pricing = _pricing->get_s3_pricing();
  const double storage_gb = used_storage_bytes / 1024.0 / 1024.0 / 1024.0;
  const auto storage_cost = pricing->monthly_price_per_stored_gb * storage_gb;

  return storage_cost;
}

double CostCalculator::calculate_cost_s3_requests(const size_t requests_tier1, const size_t requests_tier2) const {
  const auto pricing = _pricing->get_s3_pricing();
  const double requests_tier1_cost = requests_tier1 * pricing->price_per_request_tier1;
  const double requests_tier2_cost = requests_tier2 * pricing->price_per_request_tier2;

  return requests_tier1_cost + requests_tier2_cost;
}

double CostCalculator::calculate_cost_s3_select(const size_t returned_bytes, const size_t scanned_bytes) const {
  const auto pricing = _pricing->get_s3_pricing();
  const double returned_bytes_cost =
      (returned_bytes / 1024.0 / 1024.0 / 1024.0) * pricing->price_per_returned_gb_select;
  const double scanned_bytes_cost = (scanned_bytes / 1024.0 / 1024.0 / 1024.0) * pricing->price_per_scanned_gb_select;

  return returned_bytes_cost + scanned_bytes_cost;
}

}  // namespace skyrise
