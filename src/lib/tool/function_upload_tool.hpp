#pragma once
#include <string>

#include <cxxopts.hpp>

namespace skyrise {

static constexpr auto kFunctionNameOption = "name";
static constexpr auto kFunctionNameHint = "Function name, e.g., skyriseFunctionReadS3";

static constexpr auto kFunctionDescriptionOption = "description";
static constexpr auto kFunctionDescriptionHint = "Function description, e.g., 'username'";

static constexpr auto kFunctionMemoryOption = "memory_size";
static constexpr auto kFunctionMemoryHint = "Memory configuration in MB, e.g., 128";
static constexpr int kFunctionDefaultMemory = 128;

// TODO(tobodner): Adapt region and set it based on global configuration.
static constexpr auto kFunctionUploadedMessage =
    "Function uploaded to:\n\thttps://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/";

/**
 * Configures a function based on the given flags and deploys it to AWS Lambda.
 */
void FunctionUploadTool(const cxxopts::ParseResult& parse_result);

}  // namespace skyrise
