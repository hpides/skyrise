#pragma once

#include "function/function.hpp"

namespace skyrise {

class SimpleFunction : public Function {
 protected:
  aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const override;
};

}  // namespace skyrise
