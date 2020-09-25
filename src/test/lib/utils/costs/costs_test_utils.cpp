#include "costs_test_utils.hpp"

#include <aws/core/Aws.h>

namespace skyrise {

void InitAndShutDownAPI(const std::function<void()>& func) {
  Aws::SDKOptions options;

  Aws::InitAPI(options);
  { func(); }
  Aws::ShutdownAPI(options);
}

}  // namespace skyrise
