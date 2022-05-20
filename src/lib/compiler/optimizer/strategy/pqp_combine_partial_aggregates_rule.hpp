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

  /**
   * Since @param pipeline_breaking_aggregate_proxy is a pipeline breaker, it performs a final aggregation in the PQP.
   * Currently, a pipeline-breaking aggregate proxy is translated into a pipeline with a single worker. We must ensure
   * that a single worker does not import too many objects because it would timeout otherwise.
   * If @param pipeline_breaking_aggregate_proxy has too many input objects, this subroutine will
   * insert additional aggregation stages, so that the object count for the final aggregation stage is reduced below a
   * certain threshold defined by the QueryContext.
   *
   * @pre The PqpPartialAggregationRule must have executed before because additional aggregation stages combine partial
   * aggregates from preceding stages.
   */
  static bool CombineAggregates(const std::shared_ptr<AggregateOperatorProxy>& pipeline_breaking_aggregate_proxy);
};

}  // namespace skyrise
