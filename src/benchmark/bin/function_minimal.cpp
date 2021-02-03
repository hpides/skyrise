#include <thread>

#include <aws/lambda-runtime/runtime.h>

const size_t kSleep = 2000;

// This function does not inherit from the abstract Function class to keep its size at a minimum.
aws::lambda_runtime::invocation_response HandlerFunction(const aws::lambda_runtime::invocation_request& request) {
  // TODO(anyone): Increase sleep time if there are too many function warmstarts
  std::this_thread::sleep_for(std::chrono::milliseconds(kSleep));

  return aws::lambda_runtime::invocation_response::success(request.payload, "application/json");
}

int main() {
  aws::lambda_runtime::run_handler(HandlerFunction);

  return 0;
}
