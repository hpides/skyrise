#pragma once

#include "compiler/optimizer/abstract_rule.hpp"
#include "compiler/physical_query_plan/abstract_operator_proxy.hpp"

namespace skyrise {

class PqpCombinePartialAggregatesRule : public AbstractRule {
 public:
  const std::string& Name() const override;

  void ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& pqp_root) const override;

 protected:
  static bool CreateStagedAggregation(const std::shared_ptr<AbstractOperatorProxy>& operator_proxy);
};

}  // namespace skyrise
