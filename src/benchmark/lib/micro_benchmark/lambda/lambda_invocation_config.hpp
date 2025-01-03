#pragma once

#include <aws/core/Aws.h>

namespace skyrise {

struct LambdaInvocationConfig {
  Aws::String name;
  Aws::String instance_id;
  std::shared_ptr<Aws::IOStream> request_body;
};

}  // namespace skyrise
