#pragma once

#include "compiler/optimizer/abstract_rule.hpp"
#include "compiler/physical_query_plan/abstract_operator_proxy.hpp"

namespace skyrise {

/**
 * Prepares the given PQP for PqpPipelineSlicer:
 *  a) Determines operators that require data shuffling and inserts ExchangeOperatorProxy objects before those.
 *  b) Replicates ImportOperatorProxy as necessary, so that each ImportOperatorProxy has one output only.
 *     (TODO(anyone): Not yet implemented, see comment in cpp file)
 */
class PqpPipelinePreparationRule : public AbstractRule {
 public:
  const std::string& Name() const override;

  void ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& pqp_root) const override;
};

}  // namespace skyrise
