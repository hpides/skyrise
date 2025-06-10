#pragma once

#include <map>
#include <optional>
#include <regex>
#include <string>
#include <unordered_map>

#include <aws/ec2/model/InstanceType.h>
#include <aws/pricing/PricingClient.h>

namespace skyrise {

struct PricingLambda {
  long double price_request;
  long double price_gb_second;
  long double price_provisioned_gb_second;
  long double price_provisioned_concurrency_gb_second;
};

struct PricingS3 {
  long double price_request_tier1;
  long double price_request_tier2;
  long double price_returned_gb_select;
  long double price_scanned_gb_select;
  long double price_storage_tag_hours;
  long double price_storage_gb_months;  // This is the price for the first 50TB/month
};

struct PricingXray {
  long double price_per_accessed_trace;
  long double price_per_stored_trace;
};

struct Ec2Pricing {
  long double price_per_hour;
};

namespace usage_type_lambda {
inline const std::string kRequestArm{"Request-ARM"};
inline const std::string kRequestX86{"Request"};
inline const std::string kLambdaGBSecondArm{"Lambda-GB-Second-ARM"};
inline const std::string kLambdaGBSecondX86{"Lambda-GB-Second"};
inline const std::string kLambdaProvisionedGBSecondArm{"Lambda-Provisioned-GB-Second-ARM"};
inline const std::string kLambdaProvisionedGBSecondX86{"Lambda-Provisioned-GB-Second"};
inline const std::string kLambdaProvisionedConcurrencyArm{"Lambda-Provisioned-Concurrency-ARM"};
inline const std::string kLambdaProvisionedConcurrencyX86{"Lambda-Provisioned-Concurrency"};
}  // namespace usage_type_lambda

namespace usage_type_s3 {
inline const std::string kRequestTier1{"Requests-Tier1"};  // PUT, COPY, POST, LIST
inline const std::string kRequestTier2{"Requests-Tier2"};  // GET, SELECT, and all other
inline const std::string kSelectReturnedBytes{"Select-Returned-Bytes"};
inline const std::string kSelectScannedBytes{"Select-Scanned-Bytes"};
inline const std::string kTagStorage{"TagStorage-TagHrs"};
inline const std::string kTimedStorage{"TimedStorage-ByteHrs"};
}  // namespace usage_type_s3

namespace usage_type_xray {
inline const std::string kXrayTracesAccessed{"XRay-TracesAccessed"};
inline const std::string kXrayTracesStored{"XRay-TracesStored"};
}  // namespace usage_type_xray

/*
 * The Pricing class fetches and stores pricing information for the AWS services that Skyrise is built on using the AWS
 * Pricing SDK. The ClientConfiguration contains the region of the Price List endpoint to speak to. From the currently
 * available two endpoints at us-east-1 and ap-south-1, we always use us-east-1.
 */

class Pricing {
 public:
  Pricing(std::shared_ptr<const Aws::Pricing::PricingClient> pricing_client, const std::string& client_region);

  std::shared_ptr<Ec2Pricing> GetEc2Pricing(const Aws::EC2::Model::InstanceType ec2_instance_type) const;
  const std::shared_ptr<PricingLambda>& GetLambdaPricing() const;
  const std::shared_ptr<PricingS3>& GetS3Pricing() const;
  const std::shared_ptr<PricingXray>& GetXrayPricing() const;

 private:
  std::map<Aws::String, long double> FetchPricing(
      const Aws::String& service_code,
      const std::optional<Aws::EC2::Model::InstanceType>& ec2_instance_type = std::nullopt) const;
  Aws::Pricing::Model::GetProductsRequest CreateGetProductsRequest(
      const Aws::String& service_code, const Aws::String& next_token,
      const std::optional<Aws::EC2::Model::InstanceType>& ec2_instance_type = std::nullopt) const;

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
