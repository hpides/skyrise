#pragma once

#include <cstdint>

#include <stddef.h>

namespace skyrise {

enum class StorageOperation : std::uint8_t { kRead, kWrite };

enum class StorageSystem : std::uint8_t { kDynamoDb, kEfs, kS3 };

struct StorageBenchmarkParameters {
  size_t object_size_kb{0};
  size_t object_count{0};
  StorageOperation operation_type{StorageOperation::kWrite};
  StorageSystem system_type{StorageSystem::kS3};
  bool is_s3_express{false};
  bool is_s3_shared{false};
};

}  // namespace skyrise
