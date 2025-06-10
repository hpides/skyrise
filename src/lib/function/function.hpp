#pragma once

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

class Function {
 public:
  virtual ~Function() = default;

  void HandleRequest() const;

 protected:
  static void MemoryAllocationExceptionHandler();
  aws::lambda_runtime::invocation_response HandlerFunction(
      const aws::lambda_runtime::invocation_request& request) const;
  virtual aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const = 0;
};

}  // namespace skyrise
