#pragma once

#include <map>

#include <aws/core/Aws.h>
#include <aws/pricing/PricingClient.h>

namespace skyrise {

struct PricingLambda {
  double price_per_request;
  double price_per_gb_second;
  double price_per_provisioned_gb_second;
  double price_per_provisioned_concurrency_gb_second;
};

struct PricingS3 {
  double price_per_request_tier1;
  double price_per_request_tier2;
  double price_per_returned_gb_select;
  double price_per_scanned_gb_select;
  double monthly_price_per_tag;
  double monthly_price_per_stored_gb;  // This is the price for the first 50TB/month
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

  const std::shared_ptr<PricingLambda> get_lambda_pricing();
  const std::shared_ptr<PricingS3> get_s3_pricing();

 private:
  std::map<Aws::String, double> _fetch_pricing(const Aws::String& service_code) const;

  Aws::String _translate_region_to_location(const Aws::String& region) const;

  Aws::Pricing::PricingClient _client;

  Aws::String _region;

  std::shared_ptr<PricingLambda> _cached_pricing_lambda;
  std::shared_ptr<PricingS3> _cached_pricing_s3;
};

}  // namespace skyrise
