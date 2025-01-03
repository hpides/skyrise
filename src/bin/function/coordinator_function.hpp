#pragma once

#include "function/function.hpp"

namespace skyrise {

class CoordinatorFunction : public Function {
 protected:
  static bool ValidateRequest(const Aws::Utils::Json::JsonView& request);
  aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const override;
};

}  // namespace skyrise
