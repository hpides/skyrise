#pragma once

#include "compiler/optimizer/abstract_rule.hpp"
#include "compiler/physical_query_plan/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/aggregate_operator_proxy.hpp"

namespace skyrise {

class PqpCombinePartialResultsRule : public AbstractRule {
 public:
  const std::string& Name() const override;

  void ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& pqp_root) const override;

 protected:
  static bool CombinePartialAggregates(const std::shared_ptr<AggregateOperatorProxy>& pipeline_breaking_aggregate_proxy);
};

}  // namespace skyrise
