#include <iostream>

#include <aws/core/utils/logging/LogMacros.h>
#include <aws/iam/model/GetUserRequest.h>

#include "client/coordinator_client.hpp"
#include "constants.hpp"
#include "metering/request_tracker/request_tracker.hpp"
#include "tool/function_upload_tool.hpp"
#include "tool/tool_config.hpp"
#include "utils/assert.hpp"
#include "utils/signal_handler.hpp"

using namespace skyrise;  // NOLINT(google-build-using-namespace)

/**
 * The command line interface (CLI) for Skyrise. The CLI operates in one of two modes.
 * 1) Interactive mode: Triggered, when no options are passed to the binary.
 * 2) Non-interactive mode: Triggered, when options are passed to the binary.
 *    E.g., ./skyrise --help to check available options.
 */
int main(int argc, char** argv) {
  RegisterSignalHandler();
  cxxopts::ParseResult parse_result;
  try {
    cxxopts::Options cli_options = ConfigureCliOptions();
    parse_result = cli_options.parse(argc, argv);

    if (parse_result.arguments().empty()) {
      // 1) Interactive mode.
      // TODO(tobodner): Add interactive mode.
      Aws::SDKOptions options;
      auto tracker = std::make_shared<skyrise::RequestTracker>();
      tracker->Install(&options);

      Aws::InitAPI(options);
      {
        const auto client = std::make_shared<skyrise::CoordinatorClient>();
        const auto iam_client = client->GetIamClient();
        const auto outcome = iam_client->GetUser(Aws::IAM::Model::GetUserRequest{});
        if (!outcome.IsSuccess()) {
          AWS_LOGSTREAM_ERROR(kCoordinatorTag.c_str(), outcome.GetError().GetMessage());
        }
        std::cout << outcome.GetResult().GetUser().GetUserName() << std::endl;
      }
      Aws::ShutdownAPI(options);

      tracker->WriteSummaryToStream(&std::clog);
    } else {
      // 2) Non-interactive mode.
      if (parse_result.count(kHelpOption)) {
        // Print help and terminate.
        std::cout << cli_options.help() << std::endl;
        return 0;
      }

      switch (ToolOptionToEnum(parse_result[kToolOption].as<std::string>())) {
        case ToolType::kFunctionUpload:
          FunctionUploadTool(parse_result);
          break;
        default:
          Fail("The tool '" + parse_result[kToolOption].as<std::string>() + "' is not implemented!");
      }
    }
  } catch (const std::bad_optional_access& optional_error) {
    std::cerr << "Found no matching enum for name '" + parse_result[kToolOption].as<std::string>() + "'" << std::endl;
    return 1;
  } catch (const std::exception& exception) {
    // Handle cxxopts exceptions and logical errors.
    std::cerr << exception.what() << std::endl;
    return 1;
  }
  DeregisterSignalHandler();
  return 0;
}
