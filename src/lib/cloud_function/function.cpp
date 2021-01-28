#include "function.hpp"

#ifdef SKYRISE_DEBUG
#include <cstdlib>
#include <iostream>

#include "utils/string.hpp"
#endif

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
#ifdef SKYRISE_DEBUG
    if (!RunsInLambdaEnvironment()) {
      RunStandalone();
    } else {
#endif
      aws::lambda_runtime::run_handler(
          [&](const aws::lambda_runtime::invocation_request& request) { return HandlerFunction(request); });
#ifdef SKYRISE_DEBUG
    }
#endif
  }
  Aws::ShutdownAPI(options);
}

#ifdef SKYRISE_DEBUG
bool Function::RunsInLambdaEnvironment() {
  // Detect AWS Lambda execution environment based on environment variables that are be set by the runtimes.
  // See https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html.

  return std::getenv("AWS_LAMBDA_FUNCTION_NAME") != nullptr;
}

void Function::RunStandalone() const {
  std::cout << "Running cloud function locally. Reading from stdin ..." << std::endl;

  // Construct a mock request with payload from stdin.
  aws::lambda_runtime::invocation_request request;
  request.payload = StreamToString(&std::cin);

  std::cout << "---------------------------------------" << std::endl;

  // Call the handler.
  aws::lambda_runtime::invocation_response response = HandlerFunction(request);

  // Print information about the response.
  std::cout << "---------------------------------------\n"
            << "is_success     = " << (response.is_success() ? "true" : "false") << "\n"
            << "content_type   = " << response.get_content_type() << "\n"
            << "content_length = " << response.get_payload().size() << "\n\n"
            << response.get_payload() << "\n"
            << "---------------------------------------" << std::endl;
}
#endif

}  // namespace skyrise
