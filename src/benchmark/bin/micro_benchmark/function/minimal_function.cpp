#include <thread>

#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

namespace {

const std::string kConcurrencyDurationKeyPattern{"concurrency_duration_seconds\":"};
const std::string kConcurrencyString{"concurrency"};
const std::string kSleepString{"sleep"};

}  // namespace

// This function does not inherit from the abstract Function class to keep its size at a minimum.
aws::lambda_runtime::invocation_response HandlerFunction(const aws::lambda_runtime::invocation_request& request) {
  std::string request_payload = request.payload;
  const std::string concurrency_duration_value = request_payload.substr(
      request_payload.find(kConcurrencyDurationKeyPattern) + kConcurrencyDurationKeyPattern.size());
  const size_t concurrency_duration_seconds =
      std::stoull(concurrency_duration_value.substr(0, concurrency_duration_value.find_first_of(",}")));

  std::this_thread::sleep_for(std::chrono::seconds(concurrency_duration_seconds));

  const std::string response_payload =
      request_payload.replace(request_payload.find(kConcurrencyString), kConcurrencyString.size(), kSleepString);
  return aws::lambda_runtime::invocation_response::success(response_payload, "application/json");
}

}  // namespace skyrise

int main() {
  aws::lambda_runtime::run_handler(skyrise::HandlerFunction);

  return 0;
}
