#include "function_minimal.hpp"

#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

aws::lambda_runtime::invocation_response FunctionMinimal::OnHandleRequest(
    const Aws::Utils::Json::JsonView& /*request*/) const {
  return aws::lambda_runtime::invocation_response::success("success", "application/json");
}

}  // namespace skyrise

int main() {
  skyrise::FunctionMinimal function_minimal;
  function_minimal.HandleRequest();

  return 0;
}
