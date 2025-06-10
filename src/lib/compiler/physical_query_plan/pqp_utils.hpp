#pragma once

#include <memory>
#include <queue>
#include <string>
#include <unordered_set>

#include "compiler/abstract_plan_node.hpp"
#include "operator_proxy/abstract_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

enum class PqpVisitation : uint8_t { kVisitInputs, kDoNotVisitInputs };

/**
 * Calls the passed @param visitor on @param pqp and recursively on its inputs.
 * The visitor returns `PqpVisitation`, indicating whether the current operator proxy's input should be visited
 * as well. The algorithm is breadth-first search.
 * Each operator proxy is visited exactly once.
 *
 * @tparam Visitor      Functor called with every node as a param.
 *
 * @return `PqpVisitation`
 */
template <typename Node, typename Visitor>
void VisitPqp(const std::shared_ptr<Node>& pqp, Visitor visitor) {
  using AbstractOperatorProxyType =
      std::conditional_t<std::is_const_v<Node>, const AbstractOperatorProxy, AbstractOperatorProxy>;

  std::queue<std::shared_ptr<AbstractOperatorProxyType>> queue;
  queue.push(pqp);

  std::unordered_set<std::shared_ptr<AbstractOperatorProxyType>> visited_operator_proxies;

  while (!queue.empty()) {
    auto operator_proxy = queue.front();
    queue.pop();

    if (!visited_operator_proxies.insert(operator_proxy).second) {
      continue;
    }

    if (visitor(operator_proxy) == PqpVisitation::kVisitInputs) {
      if (operator_proxy->LeftInput()) {
        queue.push(operator_proxy->LeftInput());
      }
      if (operator_proxy->RightInput()) {
        queue.push(operator_proxy->RightInput());
      }
    }
  }
}

enum class PqpUpwardVisitation : uint8_t { kVisitOutputs, kDoNotVisitOutputs };

/**
 * Calls the passed @param visitor on @param pqp and recursively on each operator proxy that uses it as an output.
 * The visitor returns `PqpUpwardVisitation`, indicating whether the current operator proxy's input should be visited
 * as well.
 * Each operator proxy is visited exactly once.
 *
 * @tparam Visitor      Functor called with every operator_proxy as a param.
 *                      Returns `PqpUpwardVisitation`
 */
template <typename Visitor>
void VisitPqpUpwards(const std::shared_ptr<AbstractOperatorProxy>& pqp, Visitor visitor) {
  std::queue<std::shared_ptr<AbstractOperatorProxy>> queue;
  queue.push(pqp);

  std::unordered_set<std::shared_ptr<AbstractOperatorProxy>> visited_operator_proxies;

  while (!queue.empty()) {
    auto operator_proxy = queue.front();
    queue.pop();

    if (!visited_operator_proxies.insert(operator_proxy).second) {
      continue;
    }

    if (visitor(operator_proxy) == PqpUpwardVisitation::kVisitOutputs) {
      for (const auto& output : operator_proxy->Outputs()) {
        queue.push(output);
      };
    }
  }
}

/**
 * Traverses @param pqp from the top to the bottom and @returns all leaf operator proxies.
 */
std::vector<std::shared_ptr<AbstractOperatorProxy>> PqpFindLeaves(const std::shared_ptr<AbstractOperatorProxy>& pqp);

/**
 * Traverses @param pqp and prefixes each operator's identity with @param prefix.
 */
void PrefixOperatorProxyIdentities(const std::shared_ptr<AbstractOperatorProxy>& pqp, const std::string& prefix);

}  // namespace skyrise
