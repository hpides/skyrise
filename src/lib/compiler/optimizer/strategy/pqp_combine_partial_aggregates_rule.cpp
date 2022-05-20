#include "pqp_combine_partial_aggregates_rule.hpp"

#include <cmath>
#include <set>
#include <unordered_set>

#include "compiler/physical_query_plan/aggregate_operator_proxy.hpp"
#include "compiler/physical_query_plan/exchange_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_utils.hpp"
#include "compiler/plan_utils.hpp"
#include "expression/expression_utils.hpp"
#include "pqp_pipeline_preparation_rule.hpp"

namespace skyrise {

namespace {
size_t kMaximumInputObjectsCount = 30;
}  // namespace

const std::string& PqpCombinePartialAggregatesRule::Name() const {
  static const std::string rule_name = "PqpCombinePartialAggregatesRule";
  return rule_name;
}

void PqpCombinePartialAggregatesRule::ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& pqp_root) const {
  auto leaf_proxies = PqpFindLeaves(pqp_root);
  for (const auto& leaf_proxy : leaf_proxies) {
    VisitPqpUpwards(leaf_proxy, [&](const auto& operator_proxy) {
      if (!operator_proxy->IsPipelineBreaker()) {
        return PqpUpwardVisitation::kVisitOutputs;
      }
      if (operator_proxy->Type() == OperatorType::kAggregate) {
        [[maybe_unused]] bool success = CreateStagedAggregation(operator_proxy);
      }

      return PqpUpwardVisitation::kVisitOutputs;
    });
  }
}

bool PqpCombinePartialAggregatesRule::CreateStagedAggregation(
    const std::shared_ptr<AbstractOperatorProxy>& operator_proxy) {
  Assert(operator_proxy->Type() == OperatorType::kAggregate && operator_proxy->IsPipelineBreaker(),
         "Expected final aggregation node.");
  auto aggregate_proxy = std::static_pointer_cast<AggregateOperatorProxy>(operator_proxy);

  /**
   * (1) Check whether partial merge is required.
   */
  std::shared_ptr<AbstractOperatorProxy> current_operator_proxy = aggregate_proxy;
  if (aggregate_proxy->LeftInput()->Type() == OperatorType::kExchange) {
    current_operator_proxy = aggregate_proxy->LeftInput();
    Assert(std::static_pointer_cast<ExchangeOperatorProxy>(current_operator_proxy)->GetExchangeMode() ==
               ExchangeMode::kFullMerge,
           "Expected ExchangeMode::kFullMerge.");
  }
  if (current_operator_proxy->InputObjectsCount() <= kMaximumInputObjectsCount) {
    // Partial merge is unnecessary
    return false;
  }

  /**
   * (2) Check for existence of pre-aggregation
   */
  if (current_operator_proxy->LeftInput()->Type() != OperatorType::kAggregate ||
      current_operator_proxy->LeftInput()->IsPipelineBreaker()) {
    // To implement staged aggregation, a final aggregation must exist, as well as an associated pre-aggregation.
    return false;
  }

  /**
   * (3) Determine the number of required pre-aggregation stages.
   */

  // Calculate the minimum pre-aggregation stages count
  // For example: 3000 input objects, maximum of 50 input objects per worker.
  //   log(3000) / log(50) = 2,0466
  // -> 3 pre-aggregation stages required
  const auto pre_aggregation_stages_count =
      size_t(ceil(log(current_operator_proxy->InputObjectsCount()) / log(kMaximumInputObjectsCount)));
  // Optimize the number of inputs per worker by calculating the n-th root
  // For example: 3000 input objects, 3 stages
  //   3000 ^ (1 / 3) = 14,4225
  // -> 15 input objects per pre-aggregation-worker
  size_t input_objects_per_worker = 0;
  {
    const auto base = double(current_operator_proxy->InputObjectsCount());
    const auto exp = 1.0 / pre_aggregation_stages_count;
    const double res = pow(base, exp);
    const double ceiled = ceil(res);
    input_objects_per_worker = size_t(ceiled);
  }

  /**
   * (4) Insert pre-aggregation stages, which consist of one ExchangeOperatorProxy and one AggregateOperatorProxy.
   */

  // The pre-aggregation node from (2) is the first pre-aggregation stage. To prepare the next pre-aggregation stage,
  // output results must be merged partially. Thus, a ExchangeOperatorProxy must be inserted that specifies the
  // input object count for the next pre-aggregation stage.
  size_t next_stage_number = pre_aggregation_stages_count - 1;
  while (next_stage_number > 0) {
    // Specify input object count for next pre-agg. stage
    const size_t next_stage_input_objects_count = pow(input_objects_per_worker, next_stage_number);
    const auto exchange_operator_proxy = ExchangeOperatorProxy::Make();
    exchange_operator_proxy->SetToPartialMerge(next_stage_input_objects_count);
    PlanInsertNodeBelow<AbstractOperatorProxy>(current_operator_proxy, PlanInputSide::kLeft, exchange_operator_proxy);

    // Add next pre-aggregation stage to the plan
    const auto pre_aggregate_proxy = AggregateOperatorProxy::Make(aggregate_proxy->GroupByColumnIds(),
                                                                  ExpressionsDeepCopy(aggregate_proxy->Aggregates()));
    pre_aggregate_proxy->SetComment("Pre-Aggregate");
    pre_aggregate_proxy->SetIsPipelineBreaker(false);  // necessary?
    PlanInsertNodeBelow<AbstractOperatorProxy>(current_operator_proxy, PlanInputSide::kLeft, pre_aggregate_proxy);
    --next_stage_number;
  }

  Assert(pre_aggregation_stages_count > 1, "Expected to have inserted pre-agg stages.");
  return true;
}

}  // namespace skyrise
