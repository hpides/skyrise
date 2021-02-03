#include "function_simple.hpp"

#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

aws::lambda_runtime::invocation_response FunctionSimple::OnHandleRequest(
    const Aws::Utils::Json::JsonView& /*request*/) const {
  return aws::lambda_runtime::invocation_response::success("success", "application/json");
}

}  // namespace skyrise

int main() {
  skyrise::FunctionSimple function_simple;
  function_simple.HandleRequest();

  return 0;
}
