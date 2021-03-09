#include <thread>

#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

const std::string kSleepKeyPattern = "sleep_ms\":";

// This function does not inherit from the abstract Function class to keep its size at a minimum.
aws::lambda_runtime::invocation_response HandlerFunction(const aws::lambda_runtime::invocation_request& request) {
  const std::string sleep_key_value =
      request.payload.substr(kSleepKeyPattern.size() + request.payload.find(kSleepKeyPattern));
  const std::string sleep_ms = sleep_key_value.substr(0, sleep_key_value.find_first_of(",}"));

  std::this_thread::sleep_for(std::chrono::milliseconds(std::stoull(sleep_ms)));

  return aws::lambda_runtime::invocation_response::success(request.payload, "application/json");
}

}  // namespace skyrise

int main() {
  aws::lambda_runtime::run_handler(skyrise::HandlerFunction);

  return 0;
}
