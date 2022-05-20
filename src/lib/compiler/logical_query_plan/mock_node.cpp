/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "lqp_utils.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT(google-build-using-namespace)

namespace skyrise {

MockNode::MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& name)
    : AbstractLqpNode(LqpNodeType::kMock), name_(name), column_definitions_(column_definitions) {}

std::shared_ptr<LqpColumnExpression> MockNode::GetColumn(const std::string& column_name) const {
  const auto& column_definitions = this->GetColumnDefinitions();

  for (ColumnId column_id = 0; column_id < column_definitions.size(); ++column_id) {
    if (column_definitions[column_id].second == column_name) {
      return std::make_shared<LqpColumnExpression>(SharedFromBase(), column_id);
    }
  }

  Fail("Couldn't find column named '"s + column_name + "' in MockNode");
}

const MockNode::ColumnDefinitions& MockNode::GetColumnDefinitions() const { return column_definitions_; }

std::vector<std::shared_ptr<AbstractExpression>> MockNode::OutputExpressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain that
  // in the constructor
  if (!output_expressions_) {
    output_expressions_.emplace(column_definitions_.size() - pruned_column_ids_.size());

    auto pruned_column_ids_iter = pruned_column_ids_.begin();

    ColumnId output_column_id = 0;
    for (ColumnId stored_column_id = 0; stored_column_id < column_definitions_.size(); ++stored_column_id) {
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

bool MockNode::IsColumnNullable(const ColumnId column_id) const {
  Assert(column_id < column_definitions_.size(), "ColumnId out of range");
  return false;
}

void MockNode::SetPrunedColumnIds(const std::vector<ColumnId>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected sorted vector of ColumnIds");
  DebugAssert(std::adjacent_find(pruned_column_ids.begin(), pruned_column_ids.end()) == pruned_column_ids.end(),
              "Expected vector of unique ColumnIds");

  pruned_column_ids_ = pruned_column_ids;

  // Rebuilding this lazily the next time `OutputExpressions()` is called
  output_expressions_.reset();
}

std::shared_ptr<LqpUniqueConstraints> MockNode::UniqueConstraints() const {
  auto unique_constraints = std::make_shared<LqpUniqueConstraints>();

  for (const auto& table_key_constraint : table_key_constraints_) {
    // Discard key constraints that involve pruned column id(s).
    const auto& key_constraint_column_ids = table_key_constraint.ColumnIds();
    if (std::any_of(pruned_column_ids_.cbegin(), pruned_column_ids_.cend(),
                    [&key_constraint_column_ids](const auto& pruned_column_id) {
                      // TODO(anyone): C++20: Replace with .contains
                      return key_constraint_column_ids.find(pruned_column_id) != key_constraint_column_ids.end();
                    })) {
      continue;
    }

    // Search for output expressions that represent the TableKeyConstraint's ColumnIds
    const auto& column_expressions = FindColumnExpressions(*this, key_constraint_column_ids);
    DebugAssert(column_expressions.size() == table_key_constraint.ColumnIds().size(),
                "Unexpected count of column expressions.");

    // Create LqpUniqueConstraint
    unique_constraints->emplace_back(column_expressions);
  }

  return unique_constraints;
}

const std::vector<ColumnId>& MockNode::PrunedColumnIds() const { return pruned_column_ids_; }

const std::string& MockNode::Name() const {
  static const std::string kName = "Mock";
  return kName;
}

std::string MockNode::Description(const DescriptionMode /* mode */,
                                  const AbstractExpression::DescriptionMode /* expression_mode */) const {
  std::ostringstream stream;
  // const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[MockNode '"s << name_.value_or("Unnamed") << "'] Columns:";

  ColumnId column_id = 0;
  for (const auto& column : column_definitions_) {
    if (std::find(pruned_column_ids_.begin(), pruned_column_ids_.end(), column_id) != pruned_column_ids_.end()) {
      ++column_id;
      continue;
    }
    stream << " " << column.second;
    ++column_id;
  }

  stream << " | pruned: " << pruned_column_ids_.size() << "/" << column_definitions_.size() << " columns";

  return stream.str();
}

void MockNode::SetKeyConstraints(const TableKeyConstraints& key_constraints) {
  table_key_constraints_ = key_constraints;
}

const TableKeyConstraints& MockNode::key_constraints() const { return table_key_constraints_; }

void MockNode::SetNonTrivialFunctionalDependencies(const std::vector<FunctionalDependency>& fds) {
  functional_dependencies_ = fds;
}

std::vector<FunctionalDependency> MockNode::NonTrivialFunctionalDependencies() const {
  return functional_dependencies_;
}

size_t MockNode::OnShallowHash() const {
  size_t hash = 0;
  for (const auto& pruned_column_id : pruned_column_ids_) {
    boost::hash_combine(hash, static_cast<size_t>(pruned_column_id));
  }
  for (const auto& [type, column_name] : column_definitions_) {
    boost::hash_combine(hash, type);
    boost::hash_combine(hash, column_name);
  }
  return hash;
}

std::shared_ptr<AbstractLqpNode> MockNode::OnShallowCopy(LqpNodeMapping& /* node_mapping */) const {
  const auto mock_node = MockNode::Make(column_definitions_, name_);
  mock_node->SetKeyConstraints(table_key_constraints_);
  mock_node->SetNonTrivialFunctionalDependencies(functional_dependencies_);
  mock_node->SetPrunedColumnIds(pruned_column_ids_);
  return mock_node;
}

bool MockNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& /* node_mapping */) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);
  return column_definitions_ == mock_node.column_definitions_ && pruned_column_ids_ == mock_node.pruned_column_ids_ &&
         mock_node.name_ == name_ && mock_node.key_constraints() == table_key_constraints_ &&
         mock_node.FunctionalDependencies() == functional_dependencies_;
}

}  // namespace skyrise
