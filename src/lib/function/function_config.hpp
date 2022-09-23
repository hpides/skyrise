#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

namespace skyrise {

struct FunctionConfig {
  Aws::String function_path;
  Aws::String function_name;
  Aws::String function_description;
  size_t memory_size;
  bool is_local;

  FunctionConfig(Aws::String function_path, Aws::String function_name, size_t memory_size, bool is_local,
                 Aws::String function_description = "")
      : function_path(function_path),
        function_name(function_name),
        function_description(function_description),
        memory_size(memory_size),
        is_local(is_local) {}
};

struct FunctionDeployable {
  Aws::String function_name;
  Aws::Utils::Json::JsonValue function_code;
  Aws::String function_description;
  size_t memory_size;

  FunctionDeployable(Aws::String function_name, Aws::Utils::Json::JsonValue function_code, size_t memory_size,
                     Aws::String function_description = "")
      : function_name(function_name),
        function_code(function_code),
        function_description(function_description),
        memory_size(memory_size) {}
};

}  // namespace skyrise
