#pragma once

#include <set>

#include "abstract_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class PartitionOperatorProxy : public AbstractOperatorProxy {
 public:
  PartitionOperatorProxy(const size_t partition_count, const std::set<ColumnId>& partition_column_ids,
                         std::shared_ptr<AbstractOperatorProxy> input = nullptr);

  const std::string& Name() const override;

  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);
  virtual Aws::Utils::Json::JsonValue ToJson() const override;

 protected:
  std::shared_ptr<AbstractOperator> CreateOperatorInstance() override;

 private:
  const size_t partition_count_;
  const std::set<ColumnId> partition_column_ids_;
};

}  // namespace skyrise
