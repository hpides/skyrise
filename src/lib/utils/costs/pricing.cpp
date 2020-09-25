#include "pricing.hpp"

#include <algorithm>
#include <memory>
#include <utility>

#include <aws/core/Region.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/pricing/model/Filter.h>
#include <aws/pricing/model/GetProductsRequest.h>

#include "utils/assert.hpp"

namespace skyrise {

Pricing::Pricing(const Aws::String& region) : region_(region) {
  Aws::Client::ClientConfiguration config;
  config.region = Aws::Region::US_EAST_1;
  config.caFile = "/etc/pki/tls/certs/ca-bundle.crt";

  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();

  Assert(!credentials_provider->GetAWSCredentials().IsExpiredOrEmpty(),
         "Set valid AWS credentials via the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY");

  client_ = Aws::Pricing::PricingClient(credentials_provider, config);
}

std::shared_ptr<PricingLambda> Pricing::GetLambdaPricing() {
  if (cached_pricing_lambda_) {
    return cached_pricing_lambda_;
  }

  const Aws::String service_code = "AWSLambda";
  const auto prices_map = FetchPricing(service_code);

  const PricingLambda pricing{prices_map.at(UsageTypeLambda::Request), prices_map.at(UsageTypeLambda::LambdaGBSecond),
                              prices_map.at(UsageTypeLambda::LambdaProvisionedGBSecond),
                              prices_map.at(UsageTypeLambda::LambdaProvisionedConcurrency)};
  cached_pricing_lambda_ = std::make_shared<PricingLambda>(pricing);

  return cached_pricing_lambda_;
}

std::shared_ptr<PricingS3> Pricing::GetS3Pricing() {
  if (cached_pricing_s3_) {
    return cached_pricing_s3_;
  }

  const Aws::String service_code = "AmazonS3";
  const auto prices_map = FetchPricing(service_code);

  const PricingS3 pricing{
      prices_map.at(UsageTypeS3::RequestTier1),        prices_map.at(UsageTypeS3::RequestTier2),
      prices_map.at(UsageTypeS3::SelectReturnedBytes), prices_map.at(UsageTypeS3::SelectScannedBytes),
      prices_map.at(UsageTypeS3::TagStorage),          prices_map.at(UsageTypeS3::TimedStorage)};

  cached_pricing_s3_ = std::make_shared<PricingS3>(pricing);

  return cached_pricing_s3_;
}

std::map<Aws::String, long double> Pricing::FetchPricing(const Aws::String& service_code) const {
  const auto location = TranslateRegionToLocation(region_);

  // Create filters for Price List Service API
  Aws::Vector<Aws::Pricing::Model::Filter> filters = {Aws::Pricing::Model::Filter()
                                                          .WithType(Aws::Pricing::Model::FilterType::TERM_MATCH)
                                                          .WithField("location")
                                                          .WithValue(location)};
  // Create and send the request
  Aws::Pricing::Model::GetProductsRequest request;
  request.SetServiceCode(service_code);
  request.SetFilters(filters);

  const auto outcome = client_.GetProducts(request);
  Assert(outcome.IsSuccess(), "Price List API call was unsuccessful: " + outcome.GetError().GetMessage());

  std::map<Aws::String, long double> prices_map;
  const auto price_list = outcome.GetResult().GetPriceList();

  for (const auto& price : price_list) {
    // Parse JSON from outcome string
    const auto price_value = Aws::Utils::Json::JsonValue(price);
    const auto price_view = price_value.View();

    // Retrieve and return price
    const auto usage_type = price_view.GetObject("product").GetObject("attributes").GetString("usagetype");
    const auto price_dimensions_view = price_view.GetObject("terms")
                                           .GetObject("OnDemand")
                                           .GetAllObjects()
                                           .cbegin()
                                           ->second.GetObject("priceDimensions")
                                           .GetAllObjects();

    if (price_dimensions_view.size() > 1) {
      Aws::Vector<std::pair<size_t, double>> unit_prices;

      for (auto const& price_dimension : price_dimensions_view) {
        const size_t begin_range = std::stoi(price_dimension.second.GetString("beginRange"));
        const long double unit_price = std::stold(price_dimension.second.GetObject("pricePerUnit").GetString("USD"));
        unit_prices.emplace_back(std::make_pair(begin_range, unit_price));
      }

      std::sort(unit_prices.begin(), unit_prices.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
      prices_map.emplace(std::make_pair(usage_type, unit_prices[0].second));
    } else {
      const auto single_price = price_dimensions_view.cbegin()->second.GetObject("pricePerUnit").GetString("USD");
      prices_map.emplace(std::make_pair(usage_type, std::stod(single_price)));
    }
  }

  return prices_map;
}

Aws::String Pricing::TranslateRegionToLocation(const Aws::String& region) {
  if (region == Aws::Region::US_EAST_1)
    return "US East (N. Virginia)";
  else if (region == Aws::Region::US_EAST_2)
    return "US East (Ohio)";
  else if (region == Aws::Region::US_WEST_1)
    return "US West (N. California)";
  else if (region == Aws::Region::US_WEST_2)
    return "US West (Oregon)";
  else if (region == Aws::Region::EU_WEST_1)
    return "Europe (Ireland)";
  else if (region == Aws::Region::EU_WEST_2)
    return "Europe (London)";
  else if (region == Aws::Region::EU_WEST_3)
    return "Europe (Paris)";
  else if (region == Aws::Region::EU_CENTRAL_1)
    return "Europe (Frankfurt)";
  else if (region == Aws::Region::AP_SOUTHEAST_1)
    return "Asia Pacific (Singapore)";
  else if (region == Aws::Region::AP_SOUTHEAST_2)
    return "Asia Pacific (Sydney)";
  else if (region == Aws::Region::AP_NORTHEAST_1)
    return "Asia Pacific (Tokyo)";
  else if (region == Aws::Region::AP_NORTHEAST_2)
    return "Asia Pacific (Seoul)";
  else if (region == Aws::Region::SA_EAST_1)
    return "South America (São Paulo)";
  else if (region == Aws::Region::CA_CENTRAL_1)
    return "Canada (Central)";
  else if (region == Aws::Region::AP_SOUTH_1)
    return "Asia Pacific (Mumbai)";
  else if (region == Aws::Region::CN_NORTH_1)
    return "China (Beijing)";
  else if (region == Aws::Region::CN_NORTHWEST_1)
    return "China (Ningxia)";
  else if (region == Aws::Region::US_GOV_WEST_1)
    return "AWS GovCloud (US)";
  else if (region == Aws::Region::AF_SOUTH_1)
    return "Africa (Cape Town)";
  else if (region == Aws::Region::AP_EAST_1)
    return "Asia Pacific (Hong Kong)";
  else if (region == Aws::Region::AP_NORTHEAST_3)
    return "Asia Pacific (Osaka-Local)";
  else if (region == Aws::Region::EU_NORTH_1)
    return "Europe (Stockholm)";
  else if (region == Aws::Region::ME_SOUTH_1)
    return "Middle East (Bahrain)";
  else if (region == Aws::Region::US_GOV_EAST_1)
    return "AWS GovCloud (US-East)";
  else
    FailInput("AWS region " + region + " not supported.");
}

}  // namespace skyrise
