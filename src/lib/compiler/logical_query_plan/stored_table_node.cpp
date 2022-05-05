/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "stored_table_node.hpp"

#include <algorithm>
#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "expression/lqp_column_expression.hpp"
#include "lqp_utils.hpp"
#include "utils/assert.hpp"

namespace skyrise {

StoredTableNode::StoredTableNode(std::string table_name, std::shared_ptr<AbstractCatalog> catalog)
    : AbstractLqpNode(LqpNodeType::kStoredTable), table_name_(std::move(table_name)), catalog_(std::move(catalog)) {}

std::shared_ptr<LqpColumnExpression> StoredTableNode::get_column(const std::string& name) const {
  const auto table_schema = catalog_->GetTableSchema(table_name_);
  const auto column_id = table_schema->ColumnIdByName(name);
  return std::make_shared<LqpColumnExpression>(SharedFromBase(), column_id);
}

void StoredTableNode::SetPrunedColumnIds(const std::vector<ColumnId>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected sorted vector of ColumnIds");
  DebugAssert(std::adjacent_find(pruned_column_ids.begin(), pruned_column_ids.end()) == pruned_column_ids.end(),
              "Expected vector of unique ColumnIds");

  // It is valid for an LQP to not use any of the table's columns (e.g., SELECT 5 FROM t). We still need to include at
  // least one column in the output of this node, which is used by Table::size() to determine the number of 5's.
  const auto stored_column_count = catalog_->GetTableSchema(table_name_)->TableColumnCount();
  Assert(pruned_column_ids.size() < static_cast<size_t>(stored_column_count), "Cannot exclude all columns from Table.");

  pruned_column_ids_ = pruned_column_ids;

  // Rebuilding this lazily the next time `OutputExpressions()` is called
  output_expressions_.reset();
}

const std::vector<ColumnId>& StoredTableNode::PrunedColumnIds() const { return pruned_column_ids_; }

const std::string& StoredTableNode::Name() const {
  static const std::string kName{"StoredTable"};
  return kName;
}

std::string StoredTableNode::Description(const DescriptionMode mode,
                                         const AbstractExpression::DescriptionMode /* expression_mode */) const {
  const auto table_schema = catalog_->GetTableSchema(table_name_);
  std::ostringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');

  stream << "[StoredTable]" << separator;
  stream << "Name: '" << table_name_ << "'" << separator;
  stream << "pruned: " << pruned_column_ids_.size() << "/" << table_schema->TableColumnCount() << " column(s)";

  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> StoredTableNode::OutputExpressions() const {
  // Need to initialize the expressions lazily because (a) they will have a weak_ptr to this node and we can't obtain
  // that in the constructor and (b) because we don't have column pruning information in the constructor
  if (!output_expressions_) {
    const auto table_schema = catalog_->GetTableSchema(table_name_);

    // Build `expression_` with respect to the `pruned_column_ids_`
    const auto num_unpruned_columns = table_schema->TableColumnCount() - pruned_column_ids_.size();
    output_expressions_ = std::vector<std::shared_ptr<AbstractExpression>>(num_unpruned_columns);

    auto pruned_column_ids_iter = pruned_column_ids_.begin();
    ColumnId output_column_id = 0;
    for (ColumnId stored_column_id = 0; stored_column_id < table_schema->TableColumnCount(); ++stored_column_id) {
      // Skip `stored_column_id` if it is in the sorted vector `pruned_column_ids_`
      if (pruned_column_ids_iter != pruned_column_ids_.end() && stored_column_id == *pruned_column_ids_iter) {
        ++pruned_column_ids_iter;
        continue;
      }

      (*output_expressions_)[output_column_id] =
          std::make_shared<LqpColumnExpression>(SharedFromBase(), stored_column_id);
      ++output_column_id;
    }
  }

  return *output_expressions_;
}

bool StoredTableNode::IsColumnNullable(const ColumnId column_id) const {
  return catalog_->GetTableSchema(table_name_)->ColumnIsNullable(column_id);
}

std::shared_ptr<LqpUniqueConstraints> StoredTableNode::UniqueConstraints() const {
  auto unique_constraints = std::make_shared<LqpUniqueConstraints>();

  // Create unique constraints from selected table key constraints
  const auto& table_key_constraints = catalog_->GetTableSchema(table_name_)->KeyConstraints();

  for (const TableKeyConstraint& table_key_constraint : table_key_constraints) {
    // Discard key constraints that involve pruned column id(s).
    const auto& key_constraint_column_ids = table_key_constraint.Columns();
    if (std::all_of(pruned_column_ids_.cbegin(), pruned_column_ids_.cend(),
                    [&key_constraint_column_ids](const auto& pruned_column_id) {
                      // TODO(julianmenzler): C++20: Replace with .contains
                      return (key_constraint_column_ids.find(pruned_column_id) == key_constraint_column_ids.end());
                    })) {
      continue;
    }

    // Search for expressions representing the key constraint's ColumnIds
    const auto& column_expressions = FindColumnExpressions(*this, table_key_constraint.Columns());
    DebugAssert(column_expressions.size() == table_key_constraint.Columns().size(),
                "Unexpected count of column expressions.");

    // Create LqpUniqueConstraint
    unique_constraints->emplace_back(column_expressions);
  }

  return unique_constraints;
}

size_t StoredTableNode::OnShallowHash() const {
  size_t hash{0};
  boost::hash_combine(hash, table_name_);
  for (const auto& pruned_column_id : pruned_column_ids_) {
    boost::hash_combine(hash, static_cast<size_t>(pruned_column_id));
  }
  return hash;
}

std::shared_ptr<AbstractLqpNode> StoredTableNode::OnShallowCopy(LqpNodeMapping& /* node_mapping */) const {
  const auto copy = Make(table_name_, catalog_);
  copy->SetPrunedColumnIds(pruned_column_ids_);
  return copy;
}

bool StoredTableNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& /* node_mapping */) const {
  const auto& stored_table_node = static_cast<const StoredTableNode&>(rhs);
  return table_name_ == stored_table_node.table_name_ && pruned_column_ids_ == stored_table_node.pruned_column_ids_;
}

}  // namespace skyrise
