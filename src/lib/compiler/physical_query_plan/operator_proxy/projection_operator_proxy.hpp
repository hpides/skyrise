#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "expression/abstract_expression.hpp"

namespace skyrise {

class ProjectionOperatorProxy : public EnableMakeForPlanNode<ProjectionOperatorProxy, AbstractOperatorProxy>,
                                public AbstractOperatorProxy {
 public:
  explicit ProjectionOperatorProxy(std::vector<std::shared_ptr<AbstractExpression>> expressions);

  const std::string& Name() const override;
  std::vector<std::shared_ptr<AbstractExpression>> Expressions() const;

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
  const std::vector<std::shared_ptr<AbstractExpression>> expressions_;
};

}  // namespace skyrise
