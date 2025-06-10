#include "pricing.hpp"

#include <algorithm>
#include <utility>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/pricing/model/Filter.h>
#include <aws/pricing/model/GetProductsRequest.h>
#include <sys/utsname.h>

#include "utils/assert.hpp"

namespace skyrise {

Pricing::Pricing(std::shared_ptr<const Aws::Pricing::PricingClient> pricing_client, const std::string& client_region)
    : pricing_client_(std::move(pricing_client)), client_region_(client_region) {
  const auto pricing_lambda_map = FetchPricing("AWSLambda");
  // We price functions based on the host system's processor architecture.
  utsname cpu_information{};
  uname(&cpu_information);
  if (cpu_information.machine == std::string("aarch64")) {
    pricing_lambda_ = std::make_shared<PricingLambda>(PricingLambda{
        .price_request = pricing_lambda_map.at(usage_type_lambda::kRequestArm),
        .price_gb_second = pricing_lambda_map.at(usage_type_lambda::kLambdaGBSecondArm),
        .price_provisioned_gb_second = pricing_lambda_map.at(usage_type_lambda::kLambdaProvisionedGBSecondArm),
        .price_provisioned_concurrency_gb_second =
            pricing_lambda_map.at(usage_type_lambda::kLambdaProvisionedConcurrencyArm)});
  } else {
    pricing_lambda_ = std::make_shared<PricingLambda>(PricingLambda{
        .price_request = pricing_lambda_map.at(usage_type_lambda::kRequestX86),
        .price_gb_second = pricing_lambda_map.at(usage_type_lambda::kLambdaGBSecondX86),
        .price_provisioned_gb_second = pricing_lambda_map.at(usage_type_lambda::kLambdaProvisionedGBSecondX86),
        .price_provisioned_concurrency_gb_second =
            pricing_lambda_map.at(usage_type_lambda::kLambdaProvisionedConcurrencyX86)});
  }

  const auto pricing_s3_map = FetchPricing("AmazonS3");
  pricing_s3_ = std::make_shared<PricingS3>(
      PricingS3{.price_request_tier1 = pricing_s3_map.at(usage_type_s3::kRequestTier1),
                .price_request_tier2 = pricing_s3_map.at(usage_type_s3::kRequestTier2),
                .price_returned_gb_select = pricing_s3_map.at(usage_type_s3::kSelectReturnedBytes),
                .price_scanned_gb_select = pricing_s3_map.at(usage_type_s3::kSelectScannedBytes),
                .price_storage_tag_hours = pricing_s3_map.at(usage_type_s3::kTagStorage),
                .price_storage_gb_months = pricing_s3_map.at(usage_type_s3::kTimedStorage)});

  const auto pricing_xray_map = FetchPricing("AWSXRay");
  pricing_xray_ = std::make_shared<PricingXray>(
      PricingXray{.price_per_accessed_trace = pricing_xray_map.at(usage_type_xray::kXrayTracesAccessed),
                  .price_per_stored_trace = pricing_xray_map.at(usage_type_xray::kXrayTracesStored)});
}

const std::shared_ptr<PricingLambda>& Pricing::GetLambdaPricing() const { return pricing_lambda_; }

const std::shared_ptr<PricingS3>& Pricing::GetS3Pricing() const { return pricing_s3_; }

const std::shared_ptr<PricingXray>& Pricing::GetXrayPricing() const { return pricing_xray_; }

std::map<Aws::String, long double> Pricing::FetchPricing(
    const Aws::String& service_code, const std::optional<Aws::EC2::Model::InstanceType>& ec2_instance_type) const {
  std::map<Aws::String, long double> prices;

  Aws::String next_token;

  do {
    const auto outcome =
        pricing_client_->GetProducts(CreateGetProductsRequest(service_code, next_token, ec2_instance_type));
    Assert(outcome.IsSuccess(), "Price List API call was unsuccessful: " + outcome.GetError().GetMessage());
    next_token = outcome.GetResult().GetNextToken();

    const auto price_list = outcome.GetResult().GetPriceList();

    for (const auto& price : price_list) {
      // Parse JSON from outcome string.
      const Aws::Utils::Json::JsonValue price_value(price);
      const auto price_view = price_value.View();

      auto usage_type = price_view.GetObject("product").GetObject("attributes").GetString("usagetype");

      // Some usage types are prefixed (e.g., EUC1- for eu-central-1). We want to remove these prefixes to match billing
      // types universally.
      std::smatch prefix_match;
      if (std::regex_search(usage_type, prefix_match, kUsageTypePrefixRegex)) {
        usage_type = usage_type.substr(prefix_match[0].length());
      }

      if (price_view.GetObject("terms").KeyExists("OnDemand")) {
        const auto price_dimensions_view = price_view.GetObject("terms")
                                               .GetObject("OnDemand")
                                               .GetAllObjects()
                                               .cbegin()
                                               ->second.GetObject("priceDimensions")
                                               .GetAllObjects();

        if (price_dimensions_view.size() > 1) {
          Aws::Vector<std::pair<size_t, double>> unit_prices;

          for (auto const& price_dimension : price_dimensions_view) {
            const size_t begin_range = std::stoull(price_dimension.second.GetString("beginRange"));
            const long double unit_price =
                std::stold(price_dimension.second.GetObject("pricePerUnit").GetString("USD"));
            unit_prices.emplace_back(begin_range, unit_price);
          }

          std::ranges::sort(unit_prices, [](const auto& a, const auto& b) { return a.first < b.first; });
          prices.emplace(usage_type, unit_prices[0].second);
        } else {
          const Aws::String single_price =
              price_dimensions_view.cbegin()->second.GetObject("pricePerUnit").GetString("USD");
          prices.emplace(usage_type, std::stod(single_price));
        }
      }
    }
  } while (!next_token.empty());

  return prices;
}

Aws::Pricing::Model::GetProductsRequest Pricing::CreateGetProductsRequest(
    const Aws::String& service_code, const Aws::String& next_token,
    const std::optional<Aws::EC2::Model::InstanceType>& ec2_instance_type) const {
  // Determine location from region
  Assert(kRegionToLocation.find(client_region_) != kRegionToLocation.cend(),
         "AWS region " + client_region_ + " not supported.");

  const Aws::String& location = kRegionToLocation.at(client_region_);

  // Create filters for Price List Service API
  Aws::Vector<Aws::Pricing::Model::Filter> filters = {Aws::Pricing::Model::Filter()
                                                          .WithType(Aws::Pricing::Model::FilterType::TERM_MATCH)
                                                          .WithField("location")
                                                          .WithValue(location)};
  if (service_code == "AmazonEC2") {
    Assert(ec2_instance_type.has_value(), "Instance type can not be null.");
    const std::string instance_name =
        Aws::EC2::Model::InstanceTypeMapper::GetNameForInstanceType(ec2_instance_type.value());
    filters.push_back(Aws::Pricing::Model::Filter()
                          .WithType(Aws::Pricing::Model::FilterType::TERM_MATCH)
                          .WithField("instanceType")
                          .WithValue(instance_name));
    filters.push_back(Aws::Pricing::Model::Filter()
                          .WithType(Aws::Pricing::Model::FilterType::TERM_MATCH)
                          .WithField("usagetype")
                          .WithValue("BoxUsage:" + instance_name));
    filters.push_back(Aws::Pricing::Model::Filter()
                          .WithType(Aws::Pricing::Model::FilterType::TERM_MATCH)
                          .WithField("operation")
                          .WithValue("RunInstances"));
  }

  Aws::Pricing::Model::GetProductsRequest request;
  request.SetServiceCode(service_code);
  request.SetFilters(filters);

  if (!next_token.empty()) {
    request.SetNextToken(next_token);
  }

  return request;
}

std::shared_ptr<Ec2Pricing> Pricing::GetEc2Pricing(const Aws::EC2::Model::InstanceType ec2_instance_type) const {
  // Only fetch EC2 prices if requested and for a specific instance type.
  const auto ec2_pricing_map = FetchPricing("AmazonEC2", ec2_instance_type);
  const auto instance_name = Aws::EC2::Model::InstanceTypeMapper::GetNameForInstanceType(ec2_instance_type);
  return std::make_shared<Ec2Pricing>(Ec2Pricing{ec2_pricing_map.at("BoxUsage:" + instance_name)});
}

}  // namespace skyrise
