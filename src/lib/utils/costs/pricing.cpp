#include "pricing.hpp"

#include <algorithm>
#include <utility>

#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/pricing/model/Filter.h>
#include <aws/pricing/model/GetProductsRequest.h>

#include "utils/assert.hpp"

namespace skyrise {

Pricing::Pricing(std::shared_ptr<Client> client) : client_(std::move(client)) {
  const auto pricing_lambda_map = FetchPricing("AWSLambda");
  pricing_lambda_ = std::make_shared<PricingLambda>(PricingLambda{
      pricing_lambda_map.at(UsageTypeLambda::Request), pricing_lambda_map.at(UsageTypeLambda::LambdaGBSecond),
      pricing_lambda_map.at(UsageTypeLambda::LambdaProvisionedGBSecond),
      pricing_lambda_map.at(UsageTypeLambda::LambdaProvisionedConcurrency)});

  const auto pricing_s3_map = FetchPricing("AmazonS3");
  pricing_s3_ = std::make_shared<PricingS3>(
      PricingS3{pricing_s3_map.at(UsageTypeS3::RequestTier1), pricing_s3_map.at(UsageTypeS3::RequestTier2),
                pricing_s3_map.at(UsageTypeS3::SelectReturnedBytes), pricing_s3_map.at(UsageTypeS3::SelectScannedBytes),
                pricing_s3_map.at(UsageTypeS3::TagStorage), pricing_s3_map.at(UsageTypeS3::TimedStorage)});

  const auto pricing_xray_map = FetchPricing("AWSXRay");
  pricing_xray_ = std::make_shared<PricingXray>(PricingXray{pricing_xray_map.at(UsageTypeXray::XrayTracesAccessed),
                                                            pricing_xray_map.at(UsageTypeXray::XrayTracesStored)});
}

const std::shared_ptr<PricingLambda>& Pricing::GetLambdaPricing() { return pricing_lambda_; }

const std::shared_ptr<PricingS3>& Pricing::GetS3Pricing() { return pricing_s3_; }

const std::shared_ptr<PricingXray>& Pricing::GetXrayPricing() { return pricing_xray_; }

std::map<Aws::String, long double> Pricing::FetchPricing(const Aws::String& service_code) const {
  const auto location = TranslateRegionToLocation(client_->GetClientRegion());

  // Create filters for Price List Service API
  Aws::Vector<Aws::Pricing::Model::Filter> filters = {Aws::Pricing::Model::Filter()
                                                          .WithType(Aws::Pricing::Model::FilterType::TERM_MATCH)
                                                          .WithField("location")
                                                          .WithValue(location)};
  // Create and send the request
  Aws::Pricing::Model::GetProductsRequest request;
  request.SetServiceCode(service_code);
  request.SetFilters(filters);

  const auto outcome = client_->GetPricingClient().GetProducts(request);
  Assert(outcome.IsSuccess(), "Price List API call was unsuccessful: " + outcome.GetError().GetMessage());

  std::map<Aws::String, long double> prices_map;
  const auto price_list = outcome.GetResult().GetPriceList();

  for (const auto& price : price_list) {
    // Parse JSON from outcome string
    const auto price_value = Aws::Utils::Json::JsonValue(price);
    const auto price_view = price_value.View();

    // Retrieve and return price
    auto usage_type = price_view.GetObject("product").GetObject("attributes").GetString("usagetype");

    // TODO(anyone): Complete this list by adding all possible region prefixes
    // Remove prefix if present
    if (service_code == "AWSXRay" &&
        (usage_type.find("USE1-") == 0 || usage_type.find("EUW1-") == 0 || usage_type.find("APN1-") == 0)) {
      usage_type = usage_type.substr(5);
    }

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
    return "EU (Ireland)";
  else if (region == Aws::Region::EU_WEST_2)
    return "EU (London)";
  else if (region == Aws::Region::EU_WEST_3)
    return "EU (Paris)";
  else if (region == Aws::Region::EU_CENTRAL_1)
    return "EU (Frankfurt)";
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
  else {
    Fail("AWS region " + region + " not supported.");
  }
}

}  // namespace skyrise
