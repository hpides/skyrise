#pragma once

#include <map>
#include <regex>
#include <string>
#include <unordered_map>

#include "client/client.hpp"

namespace skyrise {

struct PricingLambda {
  long double price_request_;
  long double price_gb_second_;
  long double price_provisioned_gb_second_;
  long double price_provisioned_concurrency_gb_second_;
};

struct PricingS3 {
  long double price_request_tier1_;
  long double price_request_tier2_;
  long double price_returned_gb_select_;
  long double price_scanned_gb_select_;
  long double price_storage_tag_hours_;
  long double price_storage_gb_months_;  // This is the price for the first 50TB/month
};

struct PricingXray {
  long double price_per_accessed_trace;
  long double price_per_stored_trace;
};

namespace UsageTypeLambda {
inline const std::string Request{"Request"};
inline const std::string LambdaGBSecond{"Lambda-GB-Second"};
inline const std::string LambdaEdgeRequest{"Lambda-Edge-Request"};
inline const std::string LambdaEdgeGBSecond{"Lambda-Edge-GB-Second"};
inline const std::string LambdaProvisionedGBSecond{"Lambda-Provisioned-GB-Second"};
inline const std::string LambdaProvisionedConcurrency{"Lambda-Provisioned-Concurrency"};
}  // namespace UsageTypeLambda

namespace UsageTypeS3 {
inline const std::string RequestTier1{"Requests-Tier1"};  // POST/PUT/COPY/LIST
inline const std::string RequestTier2{"Requests-Tier2"};  // GET/SELECT and everything else
inline const std::string SelectReturnedBytes{"Select-Returned-Bytes"};
inline const std::string SelectScannedBytes{"Select-Scanned-Bytes"};
inline const std::string TagStorage{"TagStorage-TagHrs"};
inline const std::string TimedStorage{"TimedStorage-ByteHrs"};
}  // namespace UsageTypeS3

namespace UsageTypeXray {
inline const std::string XrayTracesAccessed{"XRay-TracesAccessed"};
inline const std::string XrayTracesStored{"XRay-TracesStored"};
}  // namespace UsageTypeXray

/*
 * The Pricing class fetches and stores pricing information for the AWS services that Skyrise is built on using the AWS
 * Pricing SDK. The ClientConfiguration contains the region of the Price List endpoint to speak to. From the currently
 * available two endpoints at us-east-1 and ap-south-1, we always use us-east-1.
 */

class Pricing {
 public:
  Pricing(std::shared_ptr<const Aws::Pricing::PricingClient> pricing_client, const std::string& client_region);

  const std::shared_ptr<PricingLambda>& GetLambdaPricing() const;
  const std::shared_ptr<PricingS3>& GetS3Pricing() const;
  const std::shared_ptr<PricingXray>& GetXrayPricing() const;

 private:
  std::map<Aws::String, long double> FetchPricing(const Aws::String& service_code) const;
  Aws::Pricing::Model::GetProductsRequest CreateGetProductsRequest(const Aws::String& service_code,
                                                                   const Aws::String& next_token) const;

  const std::shared_ptr<const Aws::Pricing::PricingClient> pricing_client_;
  const std::string client_region_;

  std::shared_ptr<PricingLambda> pricing_lambda_;
  std::shared_ptr<PricingS3> pricing_s3_;
  std::shared_ptr<PricingXray> pricing_xray_;

  static inline const std::unordered_map<Aws::String, Aws::String> kRegionToLocation = {
      {Aws::Region::AWS_GLOBAL, "Global"},
      {Aws::Region::US_EAST_1, "US East (N. Virginia)"},
      {Aws::Region::US_EAST_2, "US East (Ohio)"},
      {Aws::Region::US_WEST_1, "US West (N. California)"},
      {Aws::Region::US_WEST_2, "US West (Oregon)"},
      {Aws::Region::AF_SOUTH_1, "Africa (Cape Town)"},
      {Aws::Region::EU_WEST_1, "EU (Ireland)"},
      {Aws::Region::EU_WEST_2, "EU (London)"},
      {Aws::Region::EU_WEST_3, "EU (Paris)"},
      {Aws::Region::EU_CENTRAL_1, "EU (Frankfurt)"},
      {Aws::Region::EU_NORTH_1, "Europe (Stockholm)"},
      {Aws::Region::AP_EAST_1, "Asia Pacific (Hong Kong)"},
      {Aws::Region::AP_SOUTH_1, "Asia Pacific (Mumbai)"},
      {Aws::Region::AP_SOUTHEAST_1, "Asia Pacific (Singapore)"},
      {Aws::Region::AP_SOUTHEAST_2, "Asia Pacific (Sydney)"},
      {Aws::Region::AP_NORTHEAST_1, "Asia Pacific (Tokyo)"},
      {Aws::Region::AP_NORTHEAST_2, "Asia Pacific (Seoul)"},
      {Aws::Region::AP_NORTHEAST_3, "Asia Pacific (Osaka-Local)"},
      {Aws::Region::SA_EAST_1, "South America (São Paulo)"},
      {Aws::Region::CA_CENTRAL_1, "Canada (Central)"},
      {Aws::Region::CN_NORTH_1, "China (Beijing)"},
      {Aws::Region::CN_NORTHWEST_1, "China (Ningxia)"},
      {Aws::Region::ME_SOUTH_1, "Middle East (Bahrain)"},
      {Aws::Region::US_GOV_WEST_1, "AWS GovCloud (US)"},
      {Aws::Region::US_GOV_EAST_1, "AWS GovCloud (US-East)"}};

  static inline const std::regex kUsageTypePrefixRegex = std::regex("^[A-Z]{2,3}\\d?-");
};

}  // namespace skyrise
