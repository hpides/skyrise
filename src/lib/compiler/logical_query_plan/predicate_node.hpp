/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "all_type_variant.hpp"

namespace skyrise {

class AbstractExpression;

enum class ScanType { TableScan, IndexScan };

/**
 * This node type represents a filter.
 * The most common use case is to represent a regular TableScan,
 * but this node is also supposed to be used for IndexScans, for example.
 *
 * HAVING clauses of GROUP BY clauses will be translated to this node type as well.
 */
class PredicateNode : public EnableMakeForPlanNode<PredicateNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  explicit PredicateNode(const std::shared_ptr<AbstractExpression>& predicate);

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  // Forwards unique constraints from the left input node
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

  std::shared_ptr<AbstractExpression> predicate() const;

  ScanType scan_type{ScanType::TableScan};

 protected:
  size_t OnShallowHash() const override;
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;
};

}  // namespace skyrise
