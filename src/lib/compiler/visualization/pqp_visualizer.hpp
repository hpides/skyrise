/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "compiler/physical_query_plan/abstract_operator_proxy.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace skyrise {

class PqpVisualizer : public AbstractVisualizer<std::vector<std::shared_ptr<AbstractOperatorProxy>>> {
 public:
  PqpVisualizer();

  PqpVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info = {}, VizVertexInfo vertex_info = {},
                VizEdgeInfo edge_info = {});

 protected:
  void BuildGraph(const std::vector<std::shared_ptr<AbstractOperatorProxy>>& plans) override;

  void BuildSubtree(const std::shared_ptr<const AbstractOperatorProxy>& op,
                    std::unordered_set<std::shared_ptr<const AbstractOperatorProxy>>& visualized_ops);

  void BuildDataflow(const std::shared_ptr<const AbstractOperatorProxy>& from,
                     const std::shared_ptr<const AbstractOperatorProxy>& to, const InputSide side);

  void AddOperator(const std::shared_ptr<const AbstractOperatorProxy>& op);
};

}  // namespace skyrise
