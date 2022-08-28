/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "visualization/abstract_visualizer.hpp"

namespace skyrise {

class LqpVisualizer : public AbstractVisualizer<std::vector<std::shared_ptr<AbstractLqpNode>>> {
 public:
  LqpVisualizer();

  LqpVisualizer(GraphvizConfig graphviz_config, VizGraphInfo graph_info = {}, VizVertexInfo vertex_info = {},
                VizEdgeInfo edge_info = {});

 protected:
  void BuildGraph(const std::vector<std::shared_ptr<AbstractLqpNode>>& lqp_roots) override;

  void BuildSubtree(const std::shared_ptr<AbstractLqpNode>& node,
                    std::unordered_set<std::shared_ptr<const AbstractLqpNode>>& visualized_nodes,
                    ExpressionUnorderedSet& visualized_sub_queries);

  void BuildDataflow(const std::shared_ptr<AbstractLqpNode>& from, const std::shared_ptr<AbstractLqpNode>& to,
                     const InputSide side);
};

}  // namespace skyrise
