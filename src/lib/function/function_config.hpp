#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

namespace skyrise {

struct FunctionConfig {
  Aws::String path;
  Aws::String name;
  Aws::String description;
  Aws::String efs_arn;
  size_t memory_size;
  bool is_local_code;
  bool is_vpc_connected;

  FunctionConfig(Aws::String path, Aws::String name, size_t memory_size, bool is_local_code = true,
                 bool is_vpc_connected = false, const Aws::String& description = "", const Aws::String& efs_arn = "")
      : path(std::move(path)),
        name(std::move(name)),
        description(description),
        efs_arn(efs_arn),
        memory_size(memory_size),
        is_local_code(is_local_code),
        is_vpc_connected(is_vpc_connected) {}
};

struct FunctionDeployable {
  Aws::String name;
  Aws::Utils::Json::JsonValue code;
  Aws::String description;
  Aws::String efs_arn;
  size_t memory_size;
  bool is_vpc_connected;

  FunctionDeployable(Aws::String name, Aws::Utils::Json::JsonValue code, size_t memory_size, bool is_vpc_connected,
                     const Aws::String& description = "", const Aws::String& efs_arn = "")
      : name(std::move(name)),
        code(std::move(code)),
        description(description),
        efs_arn(efs_arn),
        memory_size(memory_size),
        is_vpc_connected(is_vpc_connected) {}
};

}  // namespace skyrise
