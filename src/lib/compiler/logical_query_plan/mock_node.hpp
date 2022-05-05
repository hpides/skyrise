/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <optional>
#include <string>

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"
#include "expression/lqp_column_expression.hpp"
#include "storage/table/table_key_constraint.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * Node that represents a table that has no data backing it, but may provide
 *  - (mocked) statistics
 *  - or just a column layout. It will pretend it created the columns.
 * It is useful in tests (e.g. general LQP tests, optimizer tests that just rely on statistics and not actual data) and
 * the playground
 */
class MockNode : public EnableMakeForPlanNode<MockNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  using ColumnDefinitions = std::vector<std::pair<DataType, std::string>>;

  explicit MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& init_name = {});

  std::shared_ptr<LqpColumnExpression> get_column(const std::string& column_name) const;

  const ColumnDefinitions& column_definitions() const;

  std::vector<std::shared_ptr<AbstractExpression>> OutputExpressions() const override;
  bool IsColumnNullable(const ColumnId column_id) const override;

  // Generates unique constraints from table's key constraints and pays respect to pruned columns.
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

  /**
   * Vector passed to `SetPrunedColumnIds()` needs to be sorted and unique
   */
  void SetPrunedColumnIds(const std::vector<ColumnId>& pruned_column_ids);
  const std::vector<ColumnId>& PrunedColumnIds() const;

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  // Pure container functionality: MockNode does not use key constraints internally.
  void set_key_constraints(const TableKeyConstraints& key_constraints);
  const TableKeyConstraints& key_constraints() const;

  void set_non_trivial_functional_dependencies(const std::vector<FunctionalDependency>& fds);
  // Returns the specified set of non-trivial FDs.
  std::vector<FunctionalDependency> NonTrivialFunctionalDependencies() const override;

  std::optional<std::string> name;

 protected:
  size_t OnShallowHash() const override;
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;

 private:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> output_expressions_;

  // Constructor args to keep around for deep_copy()
  ColumnDefinitions column_definitions_;
  std::vector<ColumnId> pruned_column_ids_;
  std::vector<FunctionalDependency> functional_dependencies_;
  TableKeyConstraints table_key_constraints_;
};
}  // namespace skyrise
