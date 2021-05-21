#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

class Function {
 public:
  void HandleRequest() const;

 protected:
  aws::lambda_runtime::invocation_response HandlerFunction(
      const aws::lambda_runtime::invocation_request& request) const;
  virtual aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const = 0;

#if SKYRISE_DEBUG
  static bool RunsInLambdaEnvironment();
  void RunStandalone() const;
#endif
};

}  // namespace skyrise
