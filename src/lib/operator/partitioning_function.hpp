#pragma once

#include <set>

#include <aws/core/utils/json/JsonSerializer.h>

#include "storage/table/table.hpp"

namespace skyrise {

enum class PartitioningFunctionType : uint8_t { kHash, kRange };

using PartitionedPositionLists = std::vector<std::vector<std::tuple<ChunkId, size_t>>>;

class AbstractPartitioningFunction {
 public:
  AbstractPartitioningFunction(const PartitioningFunctionType type, const std::set<ColumnId>& partition_column_ids,
                               const size_t partition_count);
  virtual ~AbstractPartitioningFunction() = default;

  virtual PartitionedPositionLists Partition(const std::shared_ptr<const Table>& table) const = 0;
  virtual Aws::Utils::Json::JsonValue ToJson() const = 0;
  static std::shared_ptr<AbstractPartitioningFunction> FromJson(const Aws::Utils::Json::JsonView& json);

  PartitioningFunctionType Type() const;
  const std::set<ColumnId>& PartitionColumnIds() const;
  size_t PartitionCount() const;

 protected:
  const PartitioningFunctionType type_;
  const std::set<ColumnId> partition_column_ids_;
  const size_t partition_count_;
};

class HashPartitioningFunction : public AbstractPartitioningFunction {
 public:
  HashPartitioningFunction(const std::set<ColumnId>& partition_column_ids, const size_t partition_count);

  PartitionedPositionLists Partition(const std::shared_ptr<const Table>& table) const override;
  Aws::Utils::Json::JsonValue ToJson() const override;
};

}  // namespace skyrise
