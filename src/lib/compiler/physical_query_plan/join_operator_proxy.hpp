#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator_proxy.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "magic_enum.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * Generic operator proxy for different join implementations, such as JoinHash or JoinNestedLoop.
 * The join implementation can be specified via SetJoinImplementation.
 */
class JoinOperatorProxy : public EnableMakeForPlanNode<JoinOperatorProxy, AbstractOperatorProxy>,
                          public AbstractOperatorProxy {
 public:
  JoinOperatorProxy(const JoinMode mode, std::shared_ptr<AbstractExpression> primary_predicate,
                    std::vector<std::shared_ptr<AbstractExpression>> secondary_predicates);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;
  bool RequiresRightInput() const override;

  JoinMode GetJoinMode() const;
  const std::shared_ptr<AbstractExpression>& PrimaryPredicate() const;
  const std::vector<std::shared_ptr<AbstractExpression>>& SecondaryPredicates() const;

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;
  size_t OutputObjectsCount() const override;
  size_t OutputColumnsCount() const override;
  // Defines the implementation of this join by setting the proxy's operator type to @param operator_type.
  void SetImplementation(OperatorType operator_type);

  /**
   * Serialization / Deserialization
   */
  Aws::Utils::Json::JsonValue ToJson() const override;
  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  const JoinMode mode_;
  const std::shared_ptr<AbstractExpression> primary_predicate_;
  const std::vector<std::shared_ptr<AbstractExpression>> secondary_predicates_;
};

}  // namespace skyrise
