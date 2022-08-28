/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "lqp_visualizer.hpp"

#include <cmath>
#include <iomanip>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "compiler/logical_query_plan/abstract_non_query_node.hpp"
#include "compiler/logical_query_plan/lqp_utils.hpp"

namespace skyrise {

LqpVisualizer::LqpVisualizer() {
  // Set defaults for this visualizer
  default_vertex_.shape = "rectangle";
}

LqpVisualizer::LqpVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                             VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void LqpVisualizer::BuildGraph(const std::vector<std::shared_ptr<AbstractLqpNode>>& lqp_roots) {
  std::unordered_set<std::shared_ptr<const AbstractLqpNode>> visualized_nodes;
  ExpressionUnorderedSet visualized_sub_queries;

  for (const auto& root : lqp_roots) {
    BuildSubtree(root, visualized_nodes, visualized_sub_queries);
  }
}

void LqpVisualizer::BuildSubtree(const std::shared_ptr<AbstractLqpNode>& node,
                                 std::unordered_set<std::shared_ptr<const AbstractLqpNode>>& visualized_nodes,
                                 ExpressionUnorderedSet& visualized_sub_queries) {
  // Avoid drawing dataflows/ops redundantly in diamond shaped Nodes
  if (visualized_nodes.find(node) != visualized_nodes.end()) return;
  visualized_nodes.insert(node);

  auto node_label = node->Description(DescriptionMode::kMultiLine);
  if (!node->Comment().empty()) {
    node_label += "\\n(" + node->Comment() + ")";
  }
  AddVertex(node, node_label);

  if (node->LeftInput()) {
    auto left_input = node->LeftInput();
    BuildSubtree(left_input, visualized_nodes, visualized_sub_queries);
    BuildDataflow(left_input, node, InputSide::Left);
  }

  if (node->RightInput()) {
    auto right_input = node->RightInput();
    BuildSubtree(right_input, visualized_nodes, visualized_sub_queries);
    BuildDataflow(right_input, node, InputSide::Right);
  }

  /**
   * TODO(anyone): Visualize subqueries, if implemented. Compare lqp_visualizer.cpp in Hyrise.
   */
}

void LqpVisualizer::BuildDataflow(const std::shared_ptr<AbstractLqpNode>& from,
                                  const std::shared_ptr<AbstractLqpNode>& to, const InputSide side) {
  float row_count = NAN;
  float row_percentage = 100.0f;
  double pen_width = 1.0;

  /**
   * TODO(anyone): Use cardinality estimation, if implemented. Compare lqp_visualizer.cpp in Hyrise.
   */

  std::stringstream label_stream;

  /**
   * Use a copy of the stream's default locale with thousands separators: Dynamically allocated raw pointers should
   * be avoided whenever possible. Unfortunately, std::locale stores pointers to the facets and does internal
   * reference counting. std::locale's destructor destructs the locale and the facets whose reference count becomes
   * zero. This forces us to use a dynamically allocated raw pointer here.
   */
  const auto& separate_thousands_locale = std::locale(label_stream.getloc(), new SeparateThousandsFacet);
  label_stream.imbue(separate_thousands_locale);

  if (!std::isnan(row_count)) {
    label_stream << " " << std::fixed << std::setprecision(1) << row_count << " row(s) | " << row_percentage
                 << "% estd.";
  } else {
    label_stream << "no est.";
  }

  std::stringstream tooltip_stream;

  // Edge Tooltip: Node Output Expressions
  tooltip_stream << "Output Expressions: \n";
  const auto& output_expressions = from->OutputExpressions();
  for (auto column_id = ColumnId{0}; column_id < output_expressions.size(); ++column_id) {
    tooltip_stream << " (" << column_id + 1 << ") ";
    tooltip_stream << output_expressions.at(column_id)->AsColumnName();
    if (from->IsColumnNullable(column_id)) tooltip_stream << " NULL";
    tooltip_stream << "\n";
  }

  if (!std::dynamic_pointer_cast<AbstractNonQueryNode>(from)) {
    // Edge Tooltip: Unique Constraints
    const auto& unique_constraints = from->UniqueConstraints();
    tooltip_stream << "\n"
                   << "Unique Constraints: \n";
    if (unique_constraints->empty()) tooltip_stream << " <none>\n";
    for (auto uc_idx = size_t{0}; uc_idx < unique_constraints->size(); ++uc_idx) {
      tooltip_stream << " (" << uc_idx + 1 << ") ";
      tooltip_stream << unique_constraints->at(uc_idx) << "\n";
    }

    // Edge Tooltip: Trivial FDs
    auto trivial_fds = std::vector<FunctionalDependency>();
    if (!unique_constraints->empty()) trivial_fds = fds_from_unique_constraints(from, unique_constraints);
    tooltip_stream << "\n"
                   << "Functional Dependencies (trivial): \n";
    if (trivial_fds.empty()) tooltip_stream << " <none>\n";
    for (auto fd_idx = size_t{0}; fd_idx < trivial_fds.size(); ++fd_idx) {
      tooltip_stream << " (" << fd_idx + 1 << ") ";
      tooltip_stream << trivial_fds.at(fd_idx) << "\n";
    }

    // Edge Tooltip: Non-trivial FDs
    const auto& fds = from->NonTrivialFunctionalDependencies();
    tooltip_stream << "\n"
                   << "Functional Dependencies (non-trivial): \n";
    if (fds.empty()) tooltip_stream << " <none>";
    for (auto fd_idx = size_t{0}; fd_idx < fds.size(); ++fd_idx) {
      tooltip_stream << " (" << fd_idx + 1 << ") ";
      tooltip_stream << fds.at(fd_idx) << "\n";
    }
  }

  VizEdgeInfo info = default_edge_;
  info.label = label_stream.str();
  info.label_tooltip = tooltip_stream.str();
  info.pen_width = pen_width;
  if (to->InputNodeCount() == 2) {
    info.arrowhead = side == InputSide::Left ? "lnormal" : "rnormal";
  }

  AddEdge(from, to, info);
}

}  // namespace skyrise
