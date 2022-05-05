/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "lqp_unique_constraint.hpp"

namespace skyrise {

using LqpMismatch = std::pair<std::shared_ptr<const AbstractLqpNode>, std::shared_ptr<const AbstractLqpNode>>;

/**
 * For two equally structured LQPs lhs and rhs, create a mapping for each node in lhs pointing to its equivalent in rhs.
 */
LqpNodeMapping LqpCreateNodeMapping(const std::shared_ptr<AbstractLqpNode>& lhs,
                                       const std::shared_ptr<AbstractLqpNode>& rhs);

/**
 * Perform a deep equality check of two LQPs.
 * @return std::nullopt if the LQPs were equal. A pair of a node in this LQP and a node in the rhs LQP that were first
 *         discovered to differ.
 */
std::optional<LqpMismatch> LqpFindSubplanMismatch(const std::shared_ptr<const AbstractLqpNode>& lhs,
                                                     const std::shared_ptr<const AbstractLqpNode>& rhs);

enum class LqpVisitation { kVisitInputs, kDoNotVisitInputs };

/**
 * Calls the passed @param visitor on @param lqp and recursively on its INPUTS. This will NOT visit subqueries.
 * The visitor returns `LqpVisitation`, indicating whether the current nodes's input should be visited
 * as well. The algorithm is breadth-first search.
 * Each node is visited exactly once.
 *
 * @tparam Visitor      Functor called with every node as a param.
 *                      Returns `LqpVisitation`
 */
template <typename Node, typename Visitor>
void VisitLqp(const std::shared_ptr<Node>& lqp, Visitor visitor) {
  using AbstractNodeType = std::conditional_t<std::is_const_v<Node>, const AbstractLqpNode, AbstractLqpNode>;

  std::queue<std::shared_ptr<AbstractNodeType>> node_queue;
  node_queue.push(lqp);

  std::unordered_set<std::shared_ptr<AbstractNodeType>> visited_nodes;

  while (!node_queue.empty()) {
    auto node = node_queue.front();
    node_queue.pop();

    if (!visited_nodes.emplace(node).second) {
      continue;
    }

    if (visitor(node) == LqpVisitation::kVisitInputs) {
      if (node->LeftInput()) {
        node_queue.push(node->LeftInput());
      }
      if (node->RightInput()) {
        node_queue.push(node->RightInput());
      }
    }
  }
}

enum class LqpUpwardVisitation { kVisitOutputs, kDoNotVisitOutputs };

/**
 * Calls the passed @param visitor on @param lqp and recursively on each node that uses it as an OUTPUT. If the LQP is
 * used as a subquery, the users of the subquery are not visited.
 * The visitor returns `LqpUpwardVisitation`, indicating whether the current nodes's input should be visited
 * as well.
 * Each node is visited exactly once.
 *
 * @tparam Visitor      Functor called with every node as a param.
 *                      Returns `LqpUpwardVisitation`
 */
template <typename Visitor>
void VisitLqpUpwards(const std::shared_ptr<AbstractLqpNode>& lqp, Visitor visitor) {
  std::queue<std::shared_ptr<AbstractLqpNode>> node_queue;
  node_queue.push(lqp);

  std::unordered_set<std::shared_ptr<AbstractLqpNode>> visited_nodes;

  while (!node_queue.empty()) {
    auto node = node_queue.front();
    node_queue.pop();

    if (!visited_nodes.emplace(node).second) {
      continue;
    }

    if (visitor(node) == LqpUpwardVisitation::kVisitOutputs) {
      for (const auto& output : node->Outputs()) node_queue.push(output);
    }
  }
}

/**
 * Traverses @param lqp from the top to the bottom and returns all nodes of the given @param type.
 */
std::vector<std::shared_ptr<AbstractLqpNode>> LqpFindNodesByType(const std::shared_ptr<AbstractLqpNode>& lqp,
                                                                 const LqpNodeType type);
/**
 * Traverses @param lqp from the top to the bottom and @returns all leaf nodes.
 */
std::vector<std::shared_ptr<AbstractLqpNode>> LqpFindLeaves(const std::shared_ptr<AbstractLqpNode>& lqp);

/**
 * @return A set of column expressions created by the given @param lqp_node, matching the given @param column_ids.
 *         This is a helper method that maps column ids from tables to the matching output expressions. Conceptually,
 *         it only works on data source nodes. Currently, these are StoredTableNodes, StaticTableNodes and MockNodes.
 */
ExpressionUnorderedSet FindColumnExpressions(const AbstractLqpNode& lqp_node,
                                               const std::unordered_set<ColumnId>& column_ids);

/**
 * @return True, if there is unique constraint in the given set of @param unique_constraints matching the given
 *         set of expressions. A unique constraint matches if it covers a subset of @param expressions.
 */
bool ContainsMatchingUniqueConstraint(const std::shared_ptr<LqpUniqueConstraints>& unique_constraints,
                                         const ExpressionUnorderedSet& expressions);

/**
 * @return A set of FDs, derived from the given @param unique_constraints and based on the output expressions of the
 *         given @param lqp node.
 */
std::vector<FunctionalDependency> FdsFromUniqueConstraints(
    const std::shared_ptr<const AbstractLqpNode>& lqp, const std::shared_ptr<LqpUniqueConstraints>& unique_constraints);

/**
 * This is a helper method that removes invalid or unnecessary FDs from the given input set @param fds by looking at
 * the @param lqp node's output expressions.
 */
void RemoveInvalidFds(const std::shared_ptr<const AbstractLqpNode>& lqp, std::vector<FunctionalDependency>& fds);

}  // namespace skyrise
