#include <iostream>

#include "coordinator.hpp"
#include "metering/request_tracker/request_tracker.hpp"
#include "tool/function_upload_tool.hpp"
#include "tool/tool_config.hpp"
#include "utils/assert.hpp"

using namespace skyrise;  // NOLINT(google-build-using-namespace)

/**
 * Entrypoint for the user, which consists of two modes.
 * 1) Non-interactive mode. Triggered when options are passed to the binary.
 *    E.g., ./skyrise --help to check available options.
 * 2) Interactive mode. Triggered when no options are passed to the binary.
 */
int main(int argc, char** argv) {
  cxxopts::ParseResult parse_result;
  try {
    cxxopts::Options cli_options = ConfigureCliOptions();
    parse_result = cli_options.parse(argc, argv);

    if (!parse_result.arguments().empty()) {
      // 1) Non-interactive tool mode.
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
    } else {
      // 2) Interactive mode.
      // TODO(anyone): Place the interactive mode here.
      Aws::SDKOptions options;
      auto tracker = std::make_shared<skyrise::RequestTracker>();
      tracker->Install(&options);

      Aws::InitAPI(options);
      {
        const auto client = std::make_shared<skyrise::Client>();
        skyrise::Coordinator coordinator(client);
        std::cout << coordinator.GetUser().GetUserName() << std::endl;
      }
      Aws::ShutdownAPI(options);

      tracker->WriteSummaryToStream(&std::clog);
    }
  } catch (const std::bad_optional_access& optional_error) {
    std::cerr << "Found no matching enum for name '" + parse_result[kToolOption].as<std::string>() + "'" << std::endl;
    return 1;
  } catch (const std::exception& exception) {
    // Handle cxxopts exceptions and logical errors.
    std::cerr << exception.what() << std::endl;
    return 1;
  }
  return 0;
}
