#include <aws/lambda-runtime/runtime.h>

#include "utils/profiling/function_host_information.hpp"

aws::lambda_runtime::invocation_response HandlerFunction(const aws::lambda_runtime::invocation_request& /*request*/) {
  skyrise::FunctionHostInformationCollectorConfiguration config;
  // TODO(anyone): Remove the `ip_private_command` once the `hostname` executable is available on the worker
  config.ip_private_command = "echo 0.0.0.0";

  skyrise::FunctionHostInformationCollector collector{config};
  skyrise::FunctionHostInformationIdentification information_identification =
      collector.CollectInformationIdentification();

  return aws::lambda_runtime::invocation_response::success(information_identification.id, "application/json");
}

int main() {
  aws::lambda_runtime::run_handler(HandlerFunction);

  return 0;
}
