#include "lqp_average_rewrite_rule.hpp"

#include "compiler/logical_query_plan/aggregate_node.hpp"
#include "compiler/logical_query_plan/alias_node.hpp"
#include "compiler/logical_query_plan/lqp_expression_utils.hpp"
#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/plan_utils.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

namespace {

void ReplaceAverages(const std::shared_ptr<AbstractLqpNode>& aggregate_node,
                     const std::shared_ptr<AbstractLqpNode>& lqp_root) {
  /**
   * (1) Create a list of all sums, counts, and averages in the AggregateNode.
   */
  std::vector<std::shared_ptr<AbstractExpression>> sums;
  std::vector<std::shared_ptr<AbstractExpression>> counts;
  std::vector<std::shared_ptr<AbstractExpression>> avgs;

  for (auto& node_expression : aggregate_node->node_expressions_) {
    if (node_expression->type_ != ExpressionType::kAggregate) continue;
    auto& aggregate_expression = static_cast<AggregateExpression&>(*node_expression);
    switch (aggregate_expression.aggregate_function_) {
      case AggregateFunction::kSum: {
        sums.emplace_back(node_expression);
        break;
      }
      case AggregateFunction::kCount: {
        counts.emplace_back(node_expression);
        break;
      }
      case AggregateFunction::kAvg: {
        avgs.emplace_back(node_expression);
        break;
      }
      default:
        continue;
    }
  }

  // Take a copy of what the aggregate was originally supposed to do.
  const auto original_aggregate_expressions = aggregate_node->OutputExpressions();
  const auto& aggregate_input_node = aggregate_node->LeftInput();

  /**
   * (2) Iterate over the AVGs, check if matching SUMs and COUNTs exist, and add a suitable replacement to
   *     `replacements`.
   */
  auto replacements = ExpressionUnorderedMap<std::shared_ptr<AbstractExpression>>{};
  auto additional_aggregates = std::vector<std::shared_ptr<AbstractExpression>>();

  for (const auto& avg_expression_ptr : avgs) {
    const auto& avg_expression = static_cast<AggregateExpression&>(*avg_expression_ptr);

    const auto& avg_argument = avg_expression.Argument();
    const auto avg_argument_is_nullable = ExpressionIsNullableOnLqp(avg_argument, *aggregate_input_node);

    // A helper function that checks whether an existing SUM matches the AVG
    const auto sum_finder = [&](const auto& other_expression) {
      const auto other_argument = static_cast<const AggregateExpression&>(*other_expression).Argument();
      return other_argument == avg_argument || (other_argument && *other_argument == *avg_argument);
    };

    // A helper function that checks whether an existing COUNT matches the AVG
    const auto count_finder = [&](const auto& other_expression) {
      const auto other_argument = static_cast<const AggregateExpression&>(*other_expression).Argument();
      const auto column_expression = std::dynamic_pointer_cast<const LqpColumnExpression>(other_argument);

      if (column_expression && column_expression->original_column_id_ == kInvalidColumnId) {
        // COUNT(*) holds an kInvalidColumnId - that is acceptable if the argument a in AVG(a) is not nullable.
        // In that case, COUNT(*) == COUNT(a).
        return !avg_argument_is_nullable;
      }
      return other_argument == avg_argument || (other_argument && *other_argument == *avg_argument);
    };

    // Find or create SUM and COUNT expressions to calculate the AVG from
    std::shared_ptr<AbstractExpression> sum;
    std::shared_ptr<AbstractExpression> count;

    auto sum_it = std::find_if(sums.begin(), sums.end(), sum_finder);
    if (sum_it != sums.end()) {
      sum = *sum_it;
    } else {
      // We need to create SUM(a) as an additional aggregate
      sum = Sum_(avg_argument);
      sums.emplace_back(sum);
      additional_aggregates.emplace_back(sum);
    }

    auto count_it = std::find_if(counts.begin(), counts.end(), count_finder);
    if (count_it != counts.end()) {
      count = *count_it;
    } else {
      // We need to create COUNT as an additional aggregate
      if (!avg_argument_is_nullable) {
        // COUNT(*) is always preferred because it can be useful for other average rewrites as well. Since it is modeled
        // as a LqpColumnExpression, we need to provide an LQP node as a parameter for technical reasons. However, any
        // leaf node is appropriate.
        std::shared_ptr<AbstractLqpNode> leaf_node = nullptr;
        VisitLqp(aggregate_node, [&](const auto& node) {
          if (!node->LeftInput()) {
            leaf_node = node;
            return LqpVisitation::kDoNotVisitInputs;
          }
          return LqpVisitation::kVisitInputs;
        });
        Assert(leaf_node, "No leaf node found below COUNT(*)");

        count = CountStarLqp_(leaf_node);
      } else {
        // COUNT(a) is required because argument is nullable
        count = Count_(avg_argument);
      }
      counts.emplace_back(count);
      additional_aggregates.emplace_back(count);
    }

    // Define AVG replacement as SUM / COUNT
    // Notes on casting:
    //  As stated in the Doc of ExpressionCommonType, the division of integer types will result in an integer result as
    //  well. Since COUNT results are always of type integer, the calculated average depends on the data type of SUM,
    //  which can be floating type or integer. To guarantee correct results, we thus cast to type double.
    replacements[avg_expression_ptr] = Div_(Cast_(sum, DataType::kDouble), count);
  }

  // No replacements possible
  if (replacements.empty()) return;

  // Back up the current column names
  const auto& root_expressions = lqp_root->OutputExpressions();
  auto old_column_names = std::vector<std::string>(root_expressions.size());
  for (auto expression_idx = size_t{0}; expression_idx < root_expressions.size(); ++expression_idx) {
    old_column_names[expression_idx] = root_expressions[expression_idx]->AsColumnName();
  }

  /**
   * (3) Remove the AVG() expression from the AggregateNode and add additional aggregates, if required by the following
   *     projection.
   */
  {
    auto& expressions = aggregate_node->node_expressions_;
    expressions.erase(
        std::remove_if(expressions.begin(), expressions.end(),
                       [&](const auto& expression) { return replacements.find(expression) != replacements.end(); }),
        expressions.end());

    expressions.insert(expressions.end(), additional_aggregates.begin(), additional_aggregates.end());
  }

  /**
   * (4) Add a ProjectionNode that calculates AVG(a) as SUM(a)/COUNT(a).
   */
  const auto projection_node = std::make_shared<ProjectionNode>(original_aggregate_expressions);
  PlanInsertNodeAbove<AbstractLqpNode>(aggregate_node, projection_node);

  /**
   * (5) Now update the AVG expression in all nodes that might refer to it, starting with the ProjectionNode
   */
  bool updated_an_alias = false;
  VisitLqpUpwards(projection_node, [&](const auto& node) {
    for (auto& expression : node->node_expressions_) {
      ExpressionDeepReplace(expression, replacements);
    }

    if (node->Type() == LqpNodeType::kAlias) updated_an_alias = true;

    return LqpUpwardVisitation::kVisitOutputs;
  });

  /**
   * (6) If there is no upward AliasNode, we need to add one that renames "SUM/COUNT" to "AVG"
   */
  if (!updated_an_alias) {
    auto root_expressions_replaced = lqp_root->OutputExpressions();

    for (auto& expression : root_expressions_replaced) {
      ExpressionDeepReplace(expression, replacements);
    }

    const auto alias_node = AliasNode::Make(root_expressions_replaced, old_column_names);
    PlanInsertNodeBelow<AbstractLqpNode>(lqp_root, PlanInputSide::kLeft, alias_node);
  }
}  // TODO(julianmenzler): Rename to RewriteAverage?

}  // namespace

const std::string& LqpAverageRewriteRule::Name() const {
  static const std::string rule_name = "LqpAverageRewriteRule";
  return rule_name;
}

void LqpAverageRewriteRule::ApplyTo(const std::shared_ptr<AbstractLqpNode>& lqp_root) const {
  VisitLqp(lqp_root, [&](const auto& node) {
    if (node->Type() == LqpNodeType::kAggregate) {
      ReplaceAverages(node, lqp_root);
    }
    return LqpVisitation::kVisitInputs;
  });
}

}  // namespace skyrise
