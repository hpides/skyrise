#pragma once

#include <aws/core/Aws.h>

namespace skyrise {

struct FunctionInvocationConfig {
  Aws::String function_name;
  Aws::String invoke_id;
  std::shared_ptr<Aws::IOStream> payload;
};

}  // namespace skyrise
