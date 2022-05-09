#include "pqp_pipeline_preparation_rule.hpp"

#include <set>
#include <unordered_set>

#include "compiler/physical_query_plan/exchange_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_utils.hpp"
#include "compiler/plan_utils.hpp"

namespace {
using namespace skyrise;  // NOLINT(google-build-using-namespace)
}  // namespace

namespace skyrise {

const std::string& PqpPipelinePreparationRule::Name() const {
  static const std::string rule_name = "PqpAggregateSplitUpRule";
  return rule_name;
}

void PqpPipelinePreparationRule::ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& pqp_root) const {
  Assert(pqp_root->Type() == OperatorType::kExport, "PQP root should have OperatorType::kExport.");
  std::unordered_set<std::shared_ptr<AbstractOperatorProxy>> visited_proxies;
  std::vector<std::shared_ptr<AbstractOperatorProxy>> pqp_leaves = PqpFindLeaves(pqp_root);

  for (const auto& pqp_leaf : pqp_leaves) {
    Assert(pqp_leaf->Type() == OperatorType::kImport, "PQP leaf should have OperatorType::kImport.");
    if (pqp_leaf->OutputNodeCount() > 1) {
      Fail("Currently, " + Name() + " accepts one output per ImportOperatorProxy only.");
      // TODO(anyone): To simplify pipeline-slicing, an ImportOperatorProxy should have one output only.
      //               Next steps: 1) Deep-copy pqp_leaf (OutputCount - 1) times.
      //                           2) Reconnect: Outputs()->at(1..n) to new ImportOperatorProxy instance from 1)
      //                           3) Write test & verify impl.
    }

    /**
     * (1) Determine pipeline breakers that require data shuffling.
     */
    VisitPqpUpwards(pqp_leaf, [&visited_proxies](const auto& operator_proxy) {
      // TODO(julianmenzler): C++20: Replace with .contains
      if (visited_proxies.find(operator_proxy) != visited_proxies.end()) {
        return PqpUpwardVisitation::kDoNotVisitOutputs;
      }
      visited_proxies.insert(operator_proxy);

      if (!operator_proxy->IsPipelineBreaker() || operator_proxy->Type() == OperatorType::kExchange) {
        // Skip data exchange operators because they already model data shuffling.
        // Also, other operators, such as filters and projections, which do not require data shuffling.
        return PqpUpwardVisitation::kVisitOutputs;
      }
      if (operator_proxy->InputObjectsCount() == 1) {
        // In case of a single input object, data shuffling is unnecessary.
        // For example: A sort operation that follows a final aggregation does not require data shuffling.
        return PqpUpwardVisitation::kVisitOutputs;
      }

      /**
       * (2) Model data shuffling: Insert ExchangeOperatorProxy before pipeline breaker's input(s).
       */
      Assert(operator_proxy->InputNodeCount() > 0,
             "Expected " + operator_proxy->Name() + " to have at least one input.");
      auto model_data_shuffling = [&operator_proxy](const PlanInputSide input_side) {
        auto input_proxy = operator_proxy->Input(input_side);
        if (input_proxy->Type() == OperatorType::kImport) {
          return;
        }
        if (input_proxy->Type() == OperatorType::kExchange) {
          return;
        }

        auto exchange_proxy = ExchangeOperatorProxy::Make();
        PlanInsertNodeBelow<AbstractOperatorProxy>(operator_proxy, input_side, exchange_proxy);
      };

      model_data_shuffling(PlanInputSide::kLeft);
      if (operator_proxy->InputNodeCount() == 2) {
        model_data_shuffling(PlanInputSide::kRight);
      }

      return PqpUpwardVisitation::kVisitOutputs;
    });
  }
}

}  // namespace skyrise
