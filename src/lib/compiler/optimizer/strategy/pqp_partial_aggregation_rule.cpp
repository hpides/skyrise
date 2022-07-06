#include "pqp_partial_aggregation_rule.hpp"

#include <numeric>

#include "compiler/physical_query_plan/aggregate_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_utils.hpp"
#include "compiler/plan_utils.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "types.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

const std::string& PqpPartialAggregationRule::Name() const {
  static const std::string rule_name = "PqpPartialAggregationRule";
  return rule_name;
}

void PqpPartialAggregationRule::ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& pqp_root) const {
  // Traverse PQP for pipeline-breaking AggregateOperatorProxies
  VisitPqp(pqp_root, [&](const auto& operator_proxy) {
    if (operator_proxy->Type() != OperatorType::kAggregate || !operator_proxy->IsPipelineBreaker()) {
      return PqpVisitation::kVisitInputs;
    }

    // Pre-aggregation should happen in parallel to be useful.
    if (operator_proxy->LeftInput()->OutputObjectsCount() == 1) {
      return PqpVisitation::kVisitInputs;
    }

    // Perform optimization, if applicable.
    auto aggregate_proxy = std::static_pointer_cast<AggregateOperatorProxy>(operator_proxy);
    if (PqpPartialAggregationRule::AllowsPartialAggregation(aggregate_proxy)) {
      PqpPartialAggregationRule::InsertPartialAggregation(aggregate_proxy);
    }

    return PqpVisitation::kVisitInputs;
  });
}

bool PqpPartialAggregationRule::AllowsPartialAggregation(
    const std::shared_ptr<AggregateOperatorProxy>& aggregate_proxy) {
  // Currently, we only support SUM, MIN, MAX, COUNT and COUNT(*) aggregates for pre-aggregation.
  for (const auto& expression : aggregate_proxy->Aggregates()) {
    Assert(expression->type_ == ExpressionType::kAggregate, "Expression should have ExpressionType::kAggregate.");
    const auto aggregate_expression = std::static_pointer_cast<AggregateExpression>(expression);

    switch (aggregate_expression->aggregate_function_) {
      case AggregateFunction::kAny:
        // Since ANY is not an actual aggregate, c.f. DependentGroupByReductionRule, it does not block pre-aggregation.
        break;
      case AggregateFunction::kAvg:
        // AVGs do not support pre-aggregation. However, they can be computed as SUM(a) / COUNT(a), which the
        // LqpAverageRewriteRule takes care of.
        return false;
      case AggregateFunction::kCount:
        break;
      case AggregateFunction::kCountDistinct:
        return false;
      case AggregateFunction::kMax:
      case AggregateFunction::kMin:
        break;
      case AggregateFunction::kStandardDeviationSample:
        return false;
      case AggregateFunction::kSum:
        break;
      default:
        Fail("Unsupported AggregateFunction.");
    }
  }
  return true;
}

void PqpPartialAggregationRule::InsertPartialAggregation(std::shared_ptr<AggregateOperatorProxy>& aggregate_proxy) {
  /**
   * (1) Create duplicate AggregateOperatorProxy for pre-aggregation, and push it below the original/final aggregation.
   */
  auto pre_aggregate_operator_proxy = AggregateOperatorProxy::Make(aggregate_proxy->GroupByColumnIds(),
                                                                   ExpressionsDeepCopy(aggregate_proxy->Aggregates()));
  pre_aggregate_operator_proxy->SetComment("Pre-Aggregate");
  std::static_pointer_cast<AggregateOperatorProxy>(pre_aggregate_operator_proxy)->SetIsPipelineBreaker(false);
  PlanInsertNodeBelow<AbstractOperatorProxy>(aggregate_proxy, PlanInputSide::kLeft, pre_aggregate_operator_proxy);

  /**
   * (2) Update final aggregation's group-by column ids to match pre-aggregation's output group-by column ids.
   */
  std::iota(aggregate_proxy->groupby_column_ids_.begin(), aggregate_proxy->groupby_column_ids_.end(), 0);

  /**
   * (3) Update final aggregation's AggregateExpressions.
   */
  size_t groupby_column_ids_count = aggregate_proxy->GroupByColumnIds().size();
  size_t aggregate_count = aggregate_proxy->Aggregates().size();
  auto& aggregates = aggregate_proxy->aggregates_;

  for (size_t i = 0; i < aggregate_count; ++i) {
    auto aggregate_expression = std::static_pointer_cast<AggregateExpression>(aggregates[i]);
    Assert(aggregate_expression->Argument()->type_ == ExpressionType::kPqpColumn,
           "Expected aggregate argument to have ExpressionType::kPqpColumn");
    const auto argument = std::static_pointer_cast<PqpColumnExpression>(aggregate_expression->Argument());

    // Update aggregate argument to match the output of the pre-aggregation.
    //  - Group-By columns are moved to the front indices.
    //  - Column data types might change in the course of the pre-aggregation.
    //    For example, a DataType::kFloat column becomes a DataType::kDouble column after a SUM aggregation.
    const ColumnId updated_column_id = i + groupby_column_ids_count;
    const DataType updated_data_type = aggregate_expression->GetDataType();
    const auto updated_argument = PqpColumn_(updated_column_id, updated_data_type, argument->is_nullable_, argument->column_name_);

    // Replace COUNT with SUM
    auto aggregate_function = aggregate_expression->aggregate_function_;
    if (aggregate_function == AggregateFunction::kCount) {
      aggregate_function = AggregateFunction::kSum;
    }

    // Replace AggregateExpression
    aggregates[i] = std::make_shared<AggregateExpression>(aggregate_function, updated_argument);
  }
}

}  // namespace skyrise
