#include "pricing.hpp"

#include <algorithm>
#include <utility>

#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/pricing/model/Filter.h>
#include <aws/pricing/model/GetProductsRequest.h>

#include "utils/assert.hpp"

namespace skyrise {

Pricing::Pricing(std::shared_ptr<const Aws::Pricing::PricingClient> pricing_client, const std::string& client_region)
    : pricing_client_(std::move(pricing_client)), client_region_(client_region) {
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

const std::shared_ptr<PricingLambda>& Pricing::GetLambdaPricing() const { return pricing_lambda_; }

const std::shared_ptr<PricingS3>& Pricing::GetS3Pricing() const { return pricing_s3_; }

const std::shared_ptr<PricingXray>& Pricing::GetXrayPricing() const { return pricing_xray_; }

std::map<Aws::String, long double> Pricing::FetchPricing(const Aws::String& service_code) const {
  std::map<Aws::String, long double> prices;

  Aws::String next_token;

  do {
    const auto outcome = pricing_client_->GetProducts(CreateGetProductsRequest(service_code, next_token));
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
          const long double unit_price = std::stold(price_dimension.second.GetObject("pricePerUnit").GetString("USD"));
          unit_prices.emplace_back(begin_range, unit_price);
        }

        std::sort(unit_prices.begin(), unit_prices.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });
        prices.emplace(usage_type, unit_prices[0].second);
      } else {
        const Aws::String single_price =
            price_dimensions_view.cbegin()->second.GetObject("pricePerUnit").GetString("USD");
        prices.emplace(usage_type, std::stod(single_price));
      }
    }
  } while (!next_token.empty());

  return prices;
}

Aws::Pricing::Model::GetProductsRequest Pricing::CreateGetProductsRequest(const Aws::String& service_code,
                                                                          const Aws::String& next_token) const {
  // Determine location from region
  Assert(kRegionToLocation.find(client_region_) != kRegionToLocation.cend(),
         "AWS region " + client_region_ + " not supported.");

  const Aws::String& location = kRegionToLocation.at(client_region_);

  // Create filters for Price List Service API
  const Aws::Vector<Aws::Pricing::Model::Filter> filters = {Aws::Pricing::Model::Filter()
                                                                .WithType(Aws::Pricing::Model::FilterType::TERM_MATCH)
                                                                .WithField("location")
                                                                .WithValue(location)};

  Aws::Pricing::Model::GetProductsRequest request;
  request.SetServiceCode(service_code);
  request.SetFilters(filters);

  if (!next_token.empty()) {
    request.SetNextToken(next_token);
  }

  return request;
}

}  // namespace skyrise
