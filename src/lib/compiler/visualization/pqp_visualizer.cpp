/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "pqp_visualizer.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

namespace skyrise {

PqpVisualizer::PqpVisualizer() = default;

PqpVisualizer::PqpVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info, VizVertexInfo vertex_info,
                             VizEdgeInfo edge_info)
    : AbstractVisualizer(std::move(graphviz_config), std::move(graph_info), std::move(vertex_info),
                         std::move(edge_info)) {}

void PqpVisualizer::BuildGraph(const std::vector<std::shared_ptr<AbstractOperatorProxy>>& plans) {
  std::unordered_set<std::shared_ptr<const AbstractOperatorProxy>> visualized_ops;

  for (const auto& plan : plans) {
    BuildSubtree(plan, visualized_ops);
  }
}

void PqpVisualizer::BuildSubtree(const std::shared_ptr<const AbstractOperatorProxy>& op,
                                 std::unordered_set<std::shared_ptr<const AbstractOperatorProxy>>& visualized_ops) {
  // Avoid drawing dataflows/ops redundantly in diamond shaped PQPs
  if (visualized_ops.find(op) != visualized_ops.end()) return;
  visualized_ops.insert(op);

  AddOperator(op);

  if (op->LeftInput()) {
    auto left = op->LeftInput();
    BuildSubtree(left, visualized_ops);
    BuildDataflow(left, op, InputSide::Left);
  }

  if (op->RightInput()) {
    auto right = op->RightInput();
    BuildSubtree(right, visualized_ops);
    BuildDataflow(right, op, InputSide::Right);
  }

  /**
   * TODO(anyone): Visualize subqueries, if implemented. Compare pqp_visualizer.cpp in Hyrise.
   */
}

void PqpVisualizer::BuildDataflow(const std::shared_ptr<const AbstractOperatorProxy>& from,
                                  const std::shared_ptr<const AbstractOperatorProxy>& to, const InputSide side) {
  VizEdgeInfo info = default_edge_;

  if (to->RightInput() != nullptr) {
    info.arrowhead = side == InputSide::Left ? "lnormal" : "rnormal";
  }

  std::stringstream label_stream;
  label_stream << from->OutputObjectsCount() << " object(s)";
  label_stream << "\n";
  label_stream << from->OutputColumnsCount() << " column(s)";
  info.label = label_stream.str();

  AddEdge(from, to, info);
}

void PqpVisualizer::AddOperator(const std::shared_ptr<const AbstractOperatorProxy>& op) {
  VizVertexInfo info = default_vertex_;
  auto label = op->Description(DescriptionMode::kMultiLine);

  info.label = label;
  AddVertex(op, info);
}

}  // namespace skyrise
