#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/lambda-runtime/runtime.h>

#include "metering/request_tracker/request_tracker.hpp"

namespace skyrise {

class Function {
 public:
  void HandleRequest() const;

 protected:
  aws::lambda_runtime::invocation_response HandlerFunction(
      const aws::lambda_runtime::invocation_request& request) const;
  static void MemoryAllocationExceptionHandler();
  virtual aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const = 0;

  inline static const std::string kTag{"SKYRISE/LOG"};

#if SKYRISE_DEBUG
  static bool RunsInLambdaEnvironment();
  static void RunStandalone(
      const std::function<aws::lambda_runtime::invocation_response(aws::lambda_runtime::invocation_request const&)>&
          handler);
#endif
};

}  // namespace skyrise
