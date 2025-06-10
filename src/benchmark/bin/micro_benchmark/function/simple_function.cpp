#include "simple_function.hpp"

#include <sstream>
#include <thread>

#include "utils/profiling/function_host_information.hpp"

namespace skyrise {

aws::lambda_runtime::invocation_response SimpleFunction::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  // Sleep for a specified amount of time to prevent instance reuse.
  size_t sleep_duration_seconds = 0;
  if (request.KeyExists("sleep") && request.KeyExists("concurrency_duration_seconds")) {
    sleep_duration_seconds = request.GetInteger("concurrency_duration_seconds");
    std::this_thread::sleep_for(std::chrono::seconds(sleep_duration_seconds));
  }

  // Collect environment ID.
  const FunctionHostInformationCollectorConfiguration config;
  FunctionHostInformationCollector collector(config);
  std::stringstream environment_id;
  environment_id << collector.CollectInformationIdentification().id;

  const auto response = Aws::Utils::Json::JsonValue()
                            .WithInteger("sleep_duration_seconds", sleep_duration_seconds)
                            .WithString("environment_id", environment_id.str());
  return aws::lambda_runtime::invocation_response::success(response.View().WriteCompact(), "application/json");
}

}  // namespace skyrise

int main() {
  const skyrise::SimpleFunction function_simple;
  function_simple.HandleRequest();

  return 0;
}
