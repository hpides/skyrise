/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_aggregate_operator.hpp"

#include <magic_enum/magic_enum.hpp>

#include "abstract_operator.hpp"
#include "expression/pqp_column_expression.hpp"
#include "types.hpp"

namespace skyrise {

AbstractAggregateOperator::AbstractAggregateOperator(std::shared_ptr<AbstractOperator> input_operator,
                                                     std::vector<std::shared_ptr<AggregateExpression>> aggregates,
                                                     std::vector<ColumnId> group_by_column_ids)
    : AbstractOperator(OperatorType::kAggregate, std::move(input_operator)),
      aggregates_(std::move(aggregates)),
      group_by_column_ids_(std::move(group_by_column_ids)) {
  // We can either have no GROUP BY columns or no aggregates, but not both.
  // No GROUP BY columns -> aggregate on the whole input table, not subgroups of it.
  // No aggregates -> effectively performs a DISTINCT operation over all GROUP BY columns.
  Assert(!(aggregates_.empty() && group_by_column_ids_.empty()),
         "Neither aggregate nor GROUP BY columns have been specified.");
}

const std::vector<std::shared_ptr<AggregateExpression>>& AbstractAggregateOperator::Aggregates() const {
  return aggregates_;
}
const std::vector<ColumnId>& AbstractAggregateOperator::GroupByColumnIds() const { return group_by_column_ids_; }

std::string AbstractAggregateOperator::Description(DescriptionMode description_mode) const {
  const auto* const separator = description_mode == DescriptionMode::kSingleLine ? " " : "\n";

  std::stringstream description;
  description << AbstractOperator::Description(description_mode) << separator << "GROUP BY column ids: ";
  for (ColumnId i = 0; i < group_by_column_ids_.size(); ++i) {
    description << group_by_column_ids_[i];

    if (i + 1 < group_by_column_ids_.size()) {
      description << ", ";
    }
  }

  description << " Aggregates: ";
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    description << aggregates_[i]->AsColumnName();

    if (i + 1 < aggregates_.size()) {
      description << ", ";
    }
  }
  return description.str();
}

/**
 * Asserts that all aggregates are valid.
 * Invalid aggregates are e.g. MAX(*) or AVG(<string column>).
 */
void AbstractAggregateOperator::ValidateAggregates() const {
  const auto input_table = LeftInputTable();
  for (const auto& aggregate : aggregates_) {
    const auto pqp_column = std::dynamic_pointer_cast<PqpColumnExpression>(aggregate->Argument());
    DebugAssert(pqp_column,
                "Aggregate operators can currently only handle physical columns, no complicated expressions.");
    const ColumnId column_id = pqp_column->GetColumnId();
    if (column_id == kInvalidColumnId) {
      Assert(aggregate->GetAggregateFunction() == AggregateFunction::kCount,
             "Aggregate: Asterisk is only valid with COUNT.");
    } else {
      DebugAssert(column_id < input_table->GetColumnCount(), "Aggregate column index out of bounds.");
      DebugAssert(pqp_column->GetDataType() == input_table->ColumnDataType(column_id),
                  "Mismatching column_data_type for input column. Table column " + std::to_string(column_id) + " has " +
                      std::string(magic_enum::enum_name(input_table->ColumnDataType(column_id))) +
                      " but the PQP expression defined " +
                      std::string(magic_enum::enum_name(pqp_column->GetDataType())));
      Assert(input_table->ColumnDataType(column_id) != DataType::kString ||
                 (aggregate->GetAggregateFunction() != AggregateFunction::kSum &&
                  aggregate->GetAggregateFunction() != AggregateFunction::kAvg &&
                  aggregate->GetAggregateFunction() != AggregateFunction::kStandardDeviationSample),
             "Aggregate: Cannot calculate SUM, AVG or STDDEV_SAMP on string column.");
    }
  }
}

}  // namespace skyrise
