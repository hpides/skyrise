#pragma once

#include <map>

#include <aws/core/Aws.h>
#include <aws/pricing/PricingClient.h>

namespace skyrise {

struct PricingLambda {
  long double price_per_request;
  long double price_per_gb_second;
  long double price_per_provisioned_gb_second;
  long double price_per_provisioned_concurrency_gb_second;
};

struct PricingS3 {
  long double price_per_request_tier1;
  long double price_per_request_tier2;
  long double price_per_returned_gb_select;
  long double price_per_scanned_gb_select;
  long double monthly_price_per_tag;
  long double monthly_price_per_stored_gb;  // This is the price for the first 50TB/month
};

namespace UsageTypeLambda {
static const char* const Request = "Request";
static const char* const LambdaGBSecond = "Lambda-GB-Second";
static const char* const LambdaEdgeRequest = "Lambda-Edge-Request";
static const char* const LambdaEdgeGBSecond = "Lambda-Edge-GB-Second";
static const char* const LambdaProvisionedGBSecond = "Lambda-Provisioned-GB-Second";
static const char* const LambdaProvisionedConcurrency = "Lambda-Provisioned-Concurrency";
}  // namespace UsageTypeLambda

namespace UsageTypeS3 {
static const char* const RequestTier1 = "Requests-Tier1";  // POST/PUT/COPY/LIST
static const char* const RequestTier2 = "Requests-Tier2";  // GET/SELECT and everything else
static const char* const SelectReturnedBytes = "Select-Returned-Bytes";
static const char* const SelectScannedBytes = "Select-Scanned-Bytes";
static const char* const TagStorage = "TagStorage-TagHrs";
static const char* const TimedStorage = "TimedStorage-ByteHrs";
}  // namespace UsageTypeS3

/*
 * The Pricing class fetches and stores pricing information for the AWS services that Skyrise is built on using the AWS
 * Pricing SDK. The ClientConfiguration contains the region of the Price List endpoint to speak to. From the currently
 * available two endpoints at us-east-1 and ap-south-1, we always use us-east-1.
 */

class Pricing {
 public:
  Pricing(const Aws::String& region);

  std::shared_ptr<PricingLambda> GetLambdaPricing();
  std::shared_ptr<PricingS3> GetS3Pricing();

 private:
  std::map<Aws::String, long double> FetchPricing(const Aws::String& service_code) const;

  static Aws::String TranslateRegionToLocation(const Aws::String& region);

  Aws::Pricing::PricingClient client_;

  Aws::String region_;

  std::shared_ptr<PricingLambda> cached_pricing_lambda_;
  std::shared_ptr<PricingS3> cached_pricing_s3_;
};

}  // namespace skyrise
