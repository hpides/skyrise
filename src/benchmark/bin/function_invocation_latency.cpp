#include <thread>

#include <aws/lambda-runtime/runtime.h>

const size_t kSleep = 2000;

// TODO(anyone): Create a general minimal function with this functionality
aws::lambda_runtime::invocation_response HandlerFunction(const aws::lambda_runtime::invocation_request& request) {
  // TODO(anyone): Increase sleep if too many function warmstarts occur
  std::this_thread::sleep_for(std::chrono::milliseconds(kSleep));

  return aws::lambda_runtime::invocation_response::success(request.payload, "application/json");
}

int main() {
  aws::lambda_runtime::run_handler(HandlerFunction);

  return 0;
}
