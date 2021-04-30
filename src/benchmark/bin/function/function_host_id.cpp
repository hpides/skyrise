#include "function_host_id.hpp"

#include <sstream>
#include <thread>

#include <aws/lambda-runtime/runtime.h>

#include "utils/profiling/function_host_information.hpp"

namespace skyrise {

inline constexpr size_t kMsSleep = 3000;

aws::lambda_runtime::invocation_response FunctionHostId::OnHandleRequest(
    const Aws::Utils::Json::JsonView& /*request*/) const {
  skyrise::FunctionHostInformationCollectorConfiguration config;

  skyrise::FunctionHostInformationCollector collector{config};
  skyrise::FunctionHostInformationIdentification information_identification =
      collector.CollectInformationIdentification();

  std::stringstream identifier;
  identifier << information_identification.id;
  identifier << "_";
  identifier << information_identification.ip_private;

  std::this_thread::sleep_for(std::chrono::milliseconds(kMsSleep));

  return aws::lambda_runtime::invocation_response::success(identifier.str(), "text/plain");
}

}  // namespace skyrise

int main() {
  skyrise::FunctionHostId function_host_id;
  function_host_id.HandleRequest();

  return 0;
}
