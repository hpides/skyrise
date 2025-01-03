#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator_proxy.hpp"
#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace skyrise {

class AggregateOperatorProxy : public EnableMakeForPlanNode<AggregateOperatorProxy, AbstractOperatorProxy>,
                               public AbstractOperatorProxy {
 public:
  AggregateOperatorProxy(std::vector<ColumnId> groupby_column_ids,
                         std::vector<std::shared_ptr<AbstractExpression>> aggregates);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  const std::vector<ColumnId>& GroupByColumnIds() const;
  const std::vector<std::shared_ptr<AbstractExpression>>& Aggregates() const;

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;
  void SetIsPipelineBreaker(bool is_pipeline_breaker);
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
  std::vector<ColumnId> groupby_column_ids_;
  std::vector<std::shared_ptr<AbstractExpression>> aggregates_;
  bool is_pipeline_breaker_{true};
};

}  // namespace skyrise
