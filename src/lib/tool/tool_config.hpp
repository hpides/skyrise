#pragma once

#include <string>

#include <cxxopts.hpp>
#include <magic_enum/magic_enum.hpp>

namespace skyrise {

enum class ToolType { kFunctionUpload };

static constexpr auto kHelpOption = "help";
static constexpr auto kHelpHint = "Print usage";

static constexpr auto kToolOption = "tool";
static constexpr auto kToolHint = "Name of the non-interactive tool, e.g., 'function-upload'";
static constexpr auto kToolRegex = "([a-z]*)-([a-z]*)";

static constexpr auto kProgramName = "skyrise";

/**
 * Transforms user input, e.g., 'function-upload' to 'kFunctionUpload' and returns the enum value.
 */
ToolType ToolOptionToEnum(const std::string& tool_option);

/**
 * Defines groups of valid CLI options which can be passed to the Skyrise binary.
 */
cxxopts::Options ConfigureCliOptions();

}  // namespace skyrise
