#include "function_upload_tool.hpp"

#include <fstream>
#include <iostream>
#include <vector>

#include "client/coordinator_client.hpp"
#include "function/function_config.hpp"
#include "function/function_utils.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

void FunctionUploadTool(const cxxopts::ParseResult& parse_result) {
  const std::string name_option = parse_result[kFunctionNameOption].as<std::string>();
  const Aws::SDKOptions options;
  const Aws::String path = "pkg/" + name_option + ".zip";
  const Aws::String name(name_option + "-" + RandomString(8));
  const int memory_size = parse_result.count(kFunctionMemoryOption) ? parse_result[kFunctionMemoryOption].as<int>()
                                                                    : kFunctionDefaultMemory;

  Assert(std::ifstream(path).good(), "Function ZIP '" + path + "' not found.");

  const FunctionConfig function_config(path, name, static_cast<size_t>(memory_size), true, false,
                                       parse_result[kFunctionDescriptionOption].as<std::string>());

  const std::vector<FunctionConfig> function_configs({function_config});

  Aws::InitAPI(options);
  const CoordinatorClient client;

  UploadFunctions(client.GetIamClient(), client.GetLambdaClient(), function_configs, false);

  Aws::ShutdownAPI(options);

  std::cout << kFunctionUploadedMessage + name << std::endl;
}
}  // namespace skyrise
