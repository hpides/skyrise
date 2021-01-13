#include "function_host_id.hpp"

#include <aws/lambda-runtime/runtime.h>

#include "utils/profiling/function_host_information.hpp"

namespace skyrise {

aws::lambda_runtime::invocation_response FunctionHostId::OnHandleRequest(
    const Aws::Utils::Json::JsonView& /*request*/) const {
  skyrise::FunctionHostInformationCollectorConfiguration config;
  // TODO(anyone): Remove the `ip_private_command` once the `hostname` executable is available on the worker
  config.ip_private_command = "echo 0.0.0.0";

  skyrise::FunctionHostInformationCollector collector{config};
  skyrise::FunctionHostInformationIdentification information_identification =
      collector.CollectInformationIdentification();

  return aws::lambda_runtime::invocation_response::success(information_identification.id, "application/json");
}

}  // namespace skyrise

int main() {
  skyrise::FunctionHostId function_host_id;
  function_host_id.HandleRequest();

  return 0;
}
