#include "string_agg_aggregate_expression.hpp"

namespace skyrise {

StringAggAggregateExpression::StringAggAggregateExpression(
    const std::shared_ptr<AbstractExpression>& argument,
    const std::optional<std::vector<SortColumnDefinition>>& sort_column_definition)
    : AggregateExpression(AggregateFunction::kStringAgg, {argument}),
      sort_column_definitions_(sort_column_definition) {}

bool StringAggAggregateExpression::OrderWithinGroup() const { return sort_column_definitions_.has_value(); }

const std::vector<SortColumnDefinition>& StringAggAggregateExpression::SortColumnDefinitions() const {
  Assert(sort_column_definitions_.has_value(), "No order by column is set.");
  return sort_column_definitions_.value();
}

}  // namespace skyrise
