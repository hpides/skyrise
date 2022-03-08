/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <array>
#include <optional>
#include <unordered_map>
#include <vector>

#include "compiler/abstract_plan_node.hpp"
#include "expression/abstract_expression.hpp"
#include "functional_dependency.hpp"
#include "lqp_unique_constraint.hpp"
#include "types.hpp"

namespace skyrise {

enum class LqpNodeType {
  kAggregate,
  kAlias,
  kDummyTable,
  kJoin,
  kLimit,
  kMock,
  kPredicate,
  kProjection,
  kRoot,
  kSort,
  kStoredTable,
  kUnion,
};

class AbstractLqpNode;
using LqpNodeMapping = std::unordered_map<std::shared_ptr<const AbstractLqpNode>, std::shared_ptr<AbstractLqpNode>>;

class AbstractLqpNode : public AbstractPlanNode<AbstractLqpNode> {
 public:
  AbstractLqpNode(const LqpNodeType node_type,
                  const std::vector<std::shared_ptr<AbstractExpression>>& init_node_expressions = {});

  LqpNodeType Type() const;

  std::string Description(const DescriptionMode mode = DescriptionMode::kSingleLine) const override;
  virtual std::string Description(const DescriptionMode mode,
                                  const AbstractExpression::DescriptionMode expression_mode) const = 0;

  /**
   * @param input_node_mapping     If the LQP contains external expressions, a mapping for the nodes used by them needs
   *                               to be provided.
   * @return                       A deep copy of the LQP this Node is the root of
   */
  std::shared_ptr<AbstractLqpNode> DeepCopy(LqpNodeMapping input_node_mapping = {}) const;

  /**
   * Compare this node with another, without comparing inputs.
   * @param node_mapping    Mapping from nodes in this node's input plans to corresponding nodes in the input plans of
   *                        rhs
   */
  bool ShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const;

  /**
   * @return The Expressions defining each "column" that this node outputs. Note: When talking about LQPs, we use the
   *         term expression, rather than column. A ProjectionNode might output `a + 5`, where a is an
   *         LqpColumnExpression and `a + 5` is an ArithmeticExpression. Avoid "column expression" if you do not mean
   *         a column that comes from an actual table.
   */
  virtual std::vector<std::shared_ptr<AbstractExpression>> OutputExpressions() const;

  /**
   * @return The ColumnId of the @param expression, or std::nullopt if it cannot be found. Note that because COUNT(*)
   *         has a special treatment (it is represented as an LqpColumnExpression with an kInvalidColumnId), it might
   *         be evaluable even if find_column_id returns nullopt.
   */
  std::optional<ColumnId> FindColumnId(const AbstractExpression& expression) const;

  /**
   * @return The ColumnId of the @param expression. Assert()s that it can be found
   */
  ColumnId GetColumnId(const AbstractExpression& expression) const;

  /**
   * @return True, if the given set of expressions is a subset of the node's output expressions. False otherwise.
   */
  bool HasOutputExpressions(const ExpressionUnorderedSet& expressions) const;

  /**
   * @return whether the output column at @param column_id is nullable
   */
  virtual bool IsColumnNullable(const ColumnId column_id) const;

  /**
   * @return Unique constraints valid for the current LQP. See lqp_unique_constraint.hpp for more documentation.
   */
  virtual std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const = 0;

  /**
   * @return True, if there is a unique constraint matching the given subset of output expressions.
   *         (i.e., the rows are guaranteed to be unique). This is preferred over calling
   *         contains_matching_unique_constraint(UniqueConstraints(), ...) as it performs additional sanity
   *         checks.
   */
  bool HasMatchingUniqueConstraint(const ExpressionUnorderedSet& expressions) const;

  /**
   * @return The functional dependencies valid for this node. See functional_dependency.hpp for documentation.
   *         They are collected from two different sources:
   *          (1) FDs derived from the node's unique constraints. (trivial FDs)
   *          (2) FDs provided by the child nodes (non-trivial FDs)
   */
  std::vector<FunctionalDependency> FunctionalDependencies() const;

  /**
   * This is a helper method that returns non-trivial FDs valid for the current node.
   * We consider FDs as non-trivial if we cannot derive them from the current node's unique constraints.
   *
   * @return The default implementation returns non-trivial FDs from the left input node, if available. Otherwise
   * an empty vector.
   *
   * Nodes should override this function
   *  - to add additional non-trivial FDs. For example, {a} -> {a + 1} (which is not yet implemented).
   *  - to discard non-trivial FDs from the input nodes, if necessary.
   *  - to specify forwarding of non-trivial FDs in case of two input nodes.
   */
  virtual std::vector<FunctionalDependency> NonTrivialFunctionalDependencies() const;

  /**
   * Perform a deep equality check
   */
  bool operator==(const AbstractLqpNode& rhs) const;
  bool operator!=(const AbstractLqpNode& rhs) const;

  /**
   * @return a hash for the (sub)plan whose root this node is
   */
  size_t hash() const;

  const LqpNodeType type_;

  /**
   * Expressions used by this node; semantics depend on the actual node type.
   * E.g., for the PredicateNode, this will be a single predicate expression; for a ProjectionNode it holds one
   * expression for each column.
   *
   * WARNING: When changing the length of this vector, **absolutely make sure** any data associated with the
   * expressions (e.g. column names in the AliasNode, SortModes in the SortNode) gets adjusted accordingly.
   */
  std::vector<std::shared_ptr<AbstractExpression>> node_expressions_;

 protected:
  /**
   * Override to hash data fields in derived types. No override needed if derived expression has no
   * data members. We do not need to take care of the input nodes here since they are already handled
   * by the calling methods.
   */
  virtual size_t OnShallowHash() const;
  virtual std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const = 0;
  virtual bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const = 0;

  /**
   * This is a helper method for node types that do not have an effect on the unique constraints from input nodes.
   * @return All unique constraints from the left input node.
   */
  std::shared_ptr<LqpUniqueConstraints> ForwardLeftUniqueConstraints() const;

 private:
  std::shared_ptr<AbstractLqpNode> DeepCopyImpl(LqpNodeMapping& node_mapping) const;
  std::shared_ptr<AbstractLqpNode> ShallowCopy(LqpNodeMapping& node_mapping) const;
};

std::ostream& operator<<(std::ostream& stream, const AbstractLqpNode& root_node);

// Wrapper around node->hash(), to enable hash-based containers containing std::shared_ptr<AbstractLqpNode>
struct LqpNodeSharedPtrHash final {
  size_t operator()(const std::shared_ptr<AbstractLqpNode>& node) const { return node->hash(); }
};

// Wrapper around AbstractLqpNode::operator==(), to enable hash-based containers containing
// std::shared_ptr<AbstractLqpNode>
struct LqpNodeSharedPtrEqual final {
  size_t operator()(const std::shared_ptr<AbstractLqpNode>& lhs, const std::shared_ptr<AbstractLqpNode>& rhs) const {
    return lhs == rhs || *lhs == *rhs;
  }
};

// Note that operator== ignores the equality function:
// https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal
template <typename Value>
using LqpNodeUnorderedMap =
    std::unordered_map<std::shared_ptr<AbstractLqpNode>, Value, LqpNodeSharedPtrHash, LqpNodeSharedPtrEqual>;

}  // namespace skyrise
