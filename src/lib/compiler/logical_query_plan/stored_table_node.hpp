/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "compiler/query_context.hpp"
#include "expression/abstract_expression.hpp"
#include "metadata/abstract_catalog.hpp"
#include "types.hpp"

namespace skyrise {

class LqpColumnExpression;

/**
 * Represents a Table and holds Column and Chunk pruning information.
 */
class StoredTableNode : public EnableMakeForPlanNode<StoredTableNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  explicit StoredTableNode(std::string table_name, std::shared_ptr<AbstractCatalog> catalog);

  std::shared_ptr<LqpColumnExpression> get_column(const std::string& name) const;

  /**
   * @defgroup ColumnIds and ChunkIds to be pruned from the table.
   * Both vectors need to be sorted and must not contain duplicates when passed to `set_pruned_{chunk/column}_ids()`
   * @{
   */
  void set_pruned_chunk_ids(const std::vector<ChunkId>& pruned_chunk_ids);
  const std::vector<ChunkId>& pruned_chunk_ids() const;

  void set_pruned_column_ids(const std::vector<ColumnId>& pruned_column_ids);
  const std::vector<ColumnId>& pruned_column_ids() const;
  /** @} */

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
  std::vector<ChunkId> pruned_chunk_ids_;  // TODO(julianmenzler): rename to pruned partition ids
  std::vector<ColumnId> pruned_column_ids_;
};

}  // namespace skyrise
