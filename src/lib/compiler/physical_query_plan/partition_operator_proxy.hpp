#pragma once

#include <set>

#include "abstract_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class PartitionOperatorProxy : public EnableMakeForPlanNode<PartitionOperatorProxy, AbstractOperatorProxy>,
                               public AbstractOperatorProxy {
 public:
  PartitionOperatorProxy(const size_t partition_count, const std::set<ColumnId>& partition_column_ids);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  size_t PartitionCount() const;
  const std::set<ColumnId>& PartitionColumnIds() const;

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;

  /**
   * Serialization / Deserialization
   */
  virtual Aws::Utils::Json::JsonValue ToJson() const override;
  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  const size_t partition_count_;
  const std::set<ColumnId> partition_column_ids_;
};

}  // namespace skyrise
