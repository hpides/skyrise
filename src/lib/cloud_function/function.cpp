#include "function.hpp"

#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/lambda-runtime/runtime.h>

namespace skyrise {

aws::lambda_runtime::invocation_response Function::HandlerFunction(
    const aws::lambda_runtime::invocation_request& request) const {
  const auto json_value = Aws::Utils::Json::JsonValue(request.payload);
  const auto json_view = json_value.View();

  const bool is_warmup = json_view.KeyExists("is_warmup") ? json_view.GetBool("is_warmup") : false;

  if (is_warmup) {
    const auto response = Aws::Utils::Json::JsonValue().WithBool("is_warmup", true);
    return aws::lambda_runtime::invocation_response::success(response.View().WriteCompact(), "application/json");
  }

  return OnHandleRequest(json_view);
}

void Function::HandleRequest() const {
  Aws::SDKOptions options;
  options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
  options.loggingOptions.logger_create_fn = [] {
    return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger",
                                                                  Aws::Utils::Logging::LogLevel::Info);
  };

  Aws::InitAPI(options);
  {
    aws::lambda_runtime::run_handler(
        [&](const aws::lambda_runtime::invocation_request& request) { return HandlerFunction(request); });
  }
  Aws::ShutdownAPI(options);
}

}  // namespace skyrise
