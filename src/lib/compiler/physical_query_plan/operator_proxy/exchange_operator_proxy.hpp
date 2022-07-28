#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_exchange_strategy.hpp"
#include "operator/partitioning_function.hpp"
#include "types.hpp"

namespace skyrise {

class ExchangeOperatorProxy : public EnableMakeForPlanNode<ExchangeOperatorProxy, AbstractOperatorProxy>,
                              public AbstractOperatorProxy {
 public:
  ExchangeOperatorProxy(AbstractExchangeStrategy strategy);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  const AbstractExchangeStrategy& Strategy() const;
  void SetStrategy(AbstractExchangeStrategy strategy);

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;
  size_t OutputObjectsCount() const override;
  size_t OutputPartitionsCount() const override;

  // Fails, because it is unsupported.
  Aws::Utils::Json::JsonValue ToJson() const override;

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  size_t ShallowHash() const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  ExchangeStrategy strategy_;
};

}  // namespace skyrise
