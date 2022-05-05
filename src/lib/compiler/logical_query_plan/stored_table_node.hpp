/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "metadata/abstract_catalog.hpp"
#include "types.hpp"

namespace skyrise {

class LqpColumnExpression;

/**
 * Represents a Table and holds Column pruning information.
 */
class StoredTableNode : public EnableMakeForPlanNode<StoredTableNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  explicit StoredTableNode(std::string table_name, std::shared_ptr<AbstractCatalog> catalog);

  std::shared_ptr<LqpColumnExpression> get_column(const std::string& name) const;

  /**
   * Vectors needs to be sorted and must not contain duplicates.
   */
  void SetPrunedColumnIds(const std::vector<ColumnId>& pruned_column_ids);
  const std::vector<ColumnId>& PrunedColumnIds() const;

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  std::vector<std::shared_ptr<AbstractExpression>> OutputExpressions() const override;
  bool IsColumnNullable(const ColumnId column_id) const override;

  // Generates unique constraints from table's key constraints and pays respect to pruned columns.
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

  const std::string table_name_;
  const std::shared_ptr<AbstractCatalog> catalog_;

 protected:
  size_t OnShallowHash() const override;
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;

 private:
  mutable std::optional<std::vector<std::shared_ptr<AbstractExpression>>> output_expressions_;
  std::vector<ColumnId> pruned_column_ids_;
};

}  // namespace skyrise
