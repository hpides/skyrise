#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class AliasOperatorProxy : public EnableMakeForPlanNode<AliasOperatorProxy, AbstractOperatorProxy>,
                           public AbstractOperatorProxy {
 public:
  AliasOperatorProxy(std::vector<ColumnId> column_ids, std::vector<std::string> aliases);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  const std::vector<ColumnId>& ColumnIds() const;
  const std::vector<std::string>& Aliases() const;

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;
  size_t OutputColumnsCount() const override;

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
  const std::vector<ColumnId> column_ids_;
  const std::vector<std::string> aliases_;
};

}  // namespace skyrise
