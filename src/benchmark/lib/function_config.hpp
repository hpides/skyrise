#pragma once

#include <aws/core/Aws.h>

namespace skyrise {

struct FunctionConfig {
  Aws::String function_path;
  Aws::String function_name;
  size_t memory_size;
  bool is_local;
};

}  // namespace skyrise
