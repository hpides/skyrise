#pragma once

#include <aws/core/Aws.h>

namespace skyrise {

class AwsAPI {
 public:
  AwsAPI() { Aws::InitAPI(options_); }
  ~AwsAPI() { Aws::ShutdownAPI(options_); }

 private:
  Aws::SDKOptions options_;
};

}  // namespace skyrise
