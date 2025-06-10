#pragma once

#include <set>

#include "abstract_operator_proxy.hpp"
#include "operator/partitioning_function.hpp"
#include "types.hpp"

namespace skyrise {

class PartitionOperatorProxy : public EnableMakeForPlanNode<PartitionOperatorProxy, AbstractOperatorProxy>,
                               public AbstractOperatorProxy {
 public:
  explicit PartitionOperatorProxy(std::shared_ptr<AbstractPartitioningFunction> partitioning_function);

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
  Aws::Utils::Json::JsonValue ToJson() const override;
  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  size_t ShallowHash() const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  const std::shared_ptr<AbstractPartitioningFunction> partitioning_function_;
};

}  // namespace skyrise
