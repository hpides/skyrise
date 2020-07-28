#include <aws/lambda-runtime/runtime.h>

aws::lambda_runtime::invocation_response HandlerFunction(const aws::lambda_runtime::invocation_request& request) {
  return aws::lambda_runtime::invocation_response::success(request.payload, "application/json");
}

int main() {
  aws::lambda_runtime::run_handler(HandlerFunction);

  return 0;
}
