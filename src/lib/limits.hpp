#pragma once

#include "utils/literal.hpp"

namespace skyrise {

// cf. https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html

const size_t kLambdaFunctionTimeoutSeconds = 900;

// Maximum file size for files to be read from the filesystem
const size_t kMaxFileSize = 2_GB;

}  // namespace skyrise
