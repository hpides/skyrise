#include <cstdlib>
#include <string>

#include <aws/core/internal/AWSHttpResourceClient.h>

namespace skyrise {

/*
 * Determine the AWS region of the function's caller.
 * If no region is found in the environment or the EC2 metadata service, an empty string is returned.
 */
std::string GetAwsRegion() {
  // Lambda provides the AWS region in the environment.
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  char* aws_region = std::getenv("AWS_REGION");

  if (!aws_region) {
    // If we are on an EC2 instance we fetch the region from the metadata service.
    Aws::Internal::InitEC2MetadataClient();
    auto client = Aws::Internal::GetEC2MetadataClient();
    return client ? client->GetCurrentRegion() : "";
  }

  return aws_region;
}
}  // namespace skyrise
