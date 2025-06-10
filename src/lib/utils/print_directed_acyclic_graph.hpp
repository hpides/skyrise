/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <ostream>

namespace skyrise {

template <typename Node>
using NodeGetChildrenFunction = std::function<std::vector<std::shared_ptr<Node>>(const std::shared_ptr<Node>&)>;

template <typename Node>
using PrintNodeFunction = std::function<void(const std::shared_ptr<Node>&, std::ostream& stream)>;

namespace detail {

/**
 * @param indentation   its size determines the indentation of a node, a true means a vertical line "|",
 *                      a false means, a space " " should be used to increase the indentation
 * @param node_to_id    used to determine whether a node was already printed and which id it has
 * @param id_counter    used to generate ids for nodes
 */
template <typename Node>
void PrintDirectedAcyclicGraphRecursively(const std::shared_ptr<Node>& node,
                                          const NodeGetChildrenFunction<Node>& get_children,
                                          const PrintNodeFunction<Node>& print_node, std::ostream& stream,
                                          std::vector<bool>& indentation,
                                          std::unordered_map<std::shared_ptr<const Node>, size_t>& node_to_id,
                                          size_t& id_counter) {
  // Indent whilst drawing the edges.
  const auto max_indentation = indentation.empty() ? 0 : indentation.size() - 1;
  for (size_t level = 0; level < max_indentation; ++level) {
    if (indentation[level]) {
      stream << " | ";
    } else {
      stream << "   ";
    }
  }

  // Only the root node is not "pointed at" with "\_<node_info>".
  if (!indentation.empty()) {
    stream << " \\_";
  }

  // Check whether the node has been printed before.
  const auto iter = node_to_id.find(node);
  if (iter != node_to_id.end()) {
    stream << "Recurring Node --> [" << iter->second << "]"
           << "\n";
    return;
  }

  const auto this_node_id = id_counter;
  ++id_counter;
  node_to_id.emplace(node, this_node_id);

  // Print node info.
  stream << "[" << this_node_id << "] ";
  print_node(node, stream);
  stream << "\n";

  const auto children = get_children(node);
  indentation.emplace_back(true);

  // Recursively progress to children.
  for (size_t i = 0; i < children.size(); ++i) {
    if (i + 1 == children.size()) {
      indentation.back() = false;
    }
    PrintDirectedAcyclicGraphRecursively<Node>(children[i], get_children, print_node, stream, indentation, node_to_id,
                                               id_counter);
  }

  indentation.pop_back();
}

}  // namespace detail

/**
 * Utility for formatted printing of any Directed Acyclic Graph.
 *
 * Results look comparable to this
 *
 * [0] [Cross Join]
 *  \_[1] [Cross Join]
 *  |  \_[2] [Predicate] a = 42
 *  |  |  \_[3] [Cross Join]
 *  |  |     \_[4] [MockTable]
 *  |  |     \_[5] [MockTable]
 *  |  \_[6] [Cross Join]
 *  |     \_Recurring Node --> [3]
 *  |     \_Recurring Node --> [5]
 *  \_[7] [Cross Join]
 *     \_Recurring Node --> [3]
 *     \_Recurring Node --> [5]
 *
 * @param node              The root node originating from which the graph should be printed
 * @param get_children   Callback that returns a nodes children
 * @param print_node     Callback that prints the information about a node, e.g. "[Predicate] a = 42" in the example
 *                              above
 * @param stream            The stream to print on
 */
template <typename Node>
void PrintDirectedAcyclicGraph(const std::shared_ptr<Node>& node, const NodeGetChildrenFunction<Node>& get_children,
                               const PrintNodeFunction<Node>& print_node, std::ostream& stream = std::cout) {
  std::vector<bool> levels;
  std::unordered_map<std::shared_ptr<Node>, size_t> node_to_id;
  size_t id_counter = 0;

  detail::PrintDirectedAcyclicGraphRecursively<Node>(node, get_children, print_node, stream, levels, node_to_id,
                                                     id_counter);
}

}  //  namespace skyrise
