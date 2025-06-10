#include "tool_config.hpp"

#include <string>

#include <boost/algorithm/string.hpp>

#include "constants.hpp"
#include "function_upload_tool.hpp"
#include "utils/assert.hpp"

namespace skyrise {

ToolType ToolOptionToEnum(const std::string& tool_option) {
  // TODO(tobodner): Find a better way.
  std::smatch match;
  std::regex_search(tool_option, match, static_cast<std::regex>(kToolRegex));
  Assert(match.size() == 3, "Tool must be specified in format 'tool-identifier'.");
  const std::string tool =
      match[1].str().replace(0, 1, boost::to_upper_copy(match[1].str().substr(0, 1))).insert(0, kConstPrefix);
  const std::string identifier = match[2].str().replace(0, 1, boost::to_upper_copy(match[2].str().substr(0, 1)));
  return magic_enum::enum_cast<ToolType>(tool + identifier).value();
}

cxxopts::Options ConfigureCliOptions() {
  cxxopts::Options cli_options(kProgramName);
  // General options.
  cli_options.add_options(kHelpOption)("h, help", kHelpHint);
  cli_options.add_options(kToolOption)("t, tool", kToolHint, cxxopts::value<std::string>());
  // Function-upload specific option group.
  cli_options.add_options("function-upload")("n, name", kFunctionNameHint, cxxopts::value<std::string>())(
      "d, description", kFunctionDescriptionHint, cxxopts::value<std::string>())("m, memory_size", kFunctionMemoryHint,
                                                                                 cxxopts::value<int>());
  return cli_options;
}

}  // namespace skyrise
