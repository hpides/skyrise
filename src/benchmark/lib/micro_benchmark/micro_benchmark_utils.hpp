#pragma once

#include <aws/core/Aws.h>

#include "micro_benchmark/micro_benchmark_types.hpp"

namespace skyrise {

std::shared_ptr<Aws::IOStream> GenerateRandomObject(const size_t num_bytes);
std::vector<StorageSystem> ParseStorageSystems(const std::vector<std::string>& storage_systems);

}  // namespace skyrise
