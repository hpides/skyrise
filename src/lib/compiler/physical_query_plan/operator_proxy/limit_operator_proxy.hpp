#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "expression/abstract_expression.hpp"

namespace skyrise {

class LimitOperatorProxy : public EnableMakeForPlanNode<LimitOperatorProxy, AbstractOperatorProxy>,
                           public AbstractOperatorProxy {
 public:
  explicit LimitOperatorProxy(std::shared_ptr<AbstractExpression> row_count);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  const std::shared_ptr<AbstractExpression>& RowCount() const;

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
  const std::shared_ptr<AbstractExpression> row_count_;
};

}  // namespace skyrise
