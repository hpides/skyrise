#include "function_simple.hpp"

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

aws::lambda_runtime::invocation_response FunctionSimple::OnHandleRequest(
    const Aws::Utils::Json::JsonView& /*request*/) const {
  const auto response_body = Aws::Utils::Json::JsonValue().WithBool("success", true);
  return aws::lambda_runtime::invocation_response::success(response_body.View().WriteCompact(), "application/json");
}

}  // namespace skyrise

int main() {
  skyrise::FunctionSimple function_simple;
  function_simple.HandleRequest();

  return 0;
}
