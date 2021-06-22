#pragma once

#include <aws/core/Aws.h>

namespace skyrise {

class AwsApi {
 public:
  AwsApi() {
    options_.httpOptions.installSigPipeHandler = true;
    Aws::InitAPI(options_);
  }
  ~AwsApi() { Aws::ShutdownAPI(options_); }

 private:
  Aws::SDKOptions options_;
};

}  // namespace skyrise
