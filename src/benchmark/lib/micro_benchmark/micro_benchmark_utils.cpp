#include "micro_benchmark_utils.hpp"

#include <aws/core/Aws.h>
#include <magic_enum/magic_enum.hpp>

#include "micro_benchmark_types.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

std::shared_ptr<Aws::IOStream> GenerateRandomObject(const size_t num_bytes) {
  return std::make_shared<Aws::StringStream>(RandomString(num_bytes));
}

std::vector<StorageSystem> ParseStorageSystems(const std::vector<std::string>& storage_systems) {
  std::vector<StorageSystem> result_types;
  result_types.reserve(storage_systems.size());

  std::ranges::transform(storage_systems, std::back_inserter(result_types), [](const std::string& storage_type) {
    const auto parsed_type = magic_enum::enum_cast<skyrise::StorageSystem>(storage_type);
    if (!parsed_type.has_value()) {
      Fail("Storage type '" + storage_type + "' cannot be parsed.");
    }
    return parsed_type.value();
  });

  return result_types;
}

}  // namespace skyrise
