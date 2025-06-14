#pragma once

#include <string>

namespace skyrise {

/*
 * Determine the AWS region of the function's caller.
 * If no region is found in the environment or the EC2 metadata service, an empty string is returned.
 */
std::string GetAwsRegion();

}  // namespace skyrise
