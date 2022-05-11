#pragma once

#include "compiler/optimizer/abstract_rule.hpp"
#include "compiler/physical_query_plan/abstract_operator_proxy.hpp"

namespace skyrise {

class AggregateOperatorProxy;

/**
 * Searches for AggregateOperatorProxies, which are defined as pipeline-breakers. Duplicates them to allow for
 * pre-aggregation.
 */
class PqpPartialAggregationRule : public AbstractRule {
 public:
  const std::string& Name() const override;
  void ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& pqp_root) const override;

 protected:
  static bool AllowsPartialAggregation(const std::shared_ptr<AggregateOperatorProxy>& aggregate_proxy);
  static void InsertPartialAggregation(std::shared_ptr<AggregateOperatorProxy>& aggregate_proxy);
};

}  // namespace skyrise
