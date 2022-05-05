#pragma once

#include "abstract_plan_node.hpp"
#include "utils/assert.hpp"

namespace skyrise {

enum AllowRightInput { kNo, kYes };

template <class NodeType>
void PlanReplaceNode(const std::shared_ptr<AbstractPlanNode<NodeType>>& original_node,
                     const std::shared_ptr<AbstractPlanNode<NodeType>>& replacement_node) {
  Assert(replacement_node->Outputs().empty(), "Node must not have outputs.");
  Assert(!replacement_node->LeftInput() && !replacement_node->RightInput(), "Replacement node must not have inputs.");

  const auto outputs = original_node->Outputs();
  const auto input_sides = original_node->GetInputSides();

  /**
   * Tie the replacement_node with this nodes inputs
   */
  replacement_node->SetLeftInput(original_node->LeftInput());
  replacement_node->SetRightInput(original_node->RightInput());

  /**
   * Tie the replacement_node with this nodes outputs.
   */
  for (size_t i = 0; i < outputs.size(); ++i) {
    outputs[i]->SetInput(input_sides[i], std::static_pointer_cast<NodeType>(replacement_node));
  }

  /**
   * Untie this node from the plan
   */
  original_node->SetLeftInput(nullptr);
  original_node->SetRightInput(nullptr);
}

template <class NodeType>
void PlanInsertNodeAbove(const std::shared_ptr<AbstractPlanNode<NodeType>>& node,
                         const std::shared_ptr<AbstractPlanNode<NodeType>>& node_to_insert,
                         const AllowRightInput allow_right_input = AllowRightInput::kNo) {
  Assert(!node_to_insert->LeftInput() && (!node_to_insert->RightInput() || allow_right_input == AllowRightInput::kYes),
         "Expected node without inputs.");

  // Re-Link @param node's outputs to @param node_to_insert
  const auto node_outputs = node->Outputs();
  for (const auto& output_node : node_outputs) {
    const PlanInputSide input_side = node->GetInputSide(output_node);
    output_node->SetInput(input_side, std::static_pointer_cast<NodeType>(node_to_insert));
  }

  // Place @param node_to_insert above @param node
  node_to_insert->SetLeftInput(std::static_pointer_cast<NodeType>(node));
}

template <class NodeType>
void PlanInsertNodeBelow(const std::shared_ptr<AbstractPlanNode<NodeType>>& parent_node, const PlanInputSide input_side,
                         const std::shared_ptr<AbstractPlanNode<NodeType>>& node_to_insert,
                         const AllowRightInput allow_right_input = AllowRightInput::kNo) {
  Assert(!node_to_insert->LeftInput() &&
             (!node_to_insert->RightInput() || allow_right_input == AllowRightInput::kYes) &&
             node_to_insert->OutputNodeCount() == 0,
         "Expected node without inputs and outputs.");

  const auto old_input = parent_node->Input(input_side);
  parent_node->SetInput(input_side, std::static_pointer_cast<NodeType>(node_to_insert));
  node_to_insert->SetLeftInput(old_input);
}

/**
 * Removes a node from the plan, using the output of its left input as input for its output nodes. Unless
 * allow_right_input is set, the node must not have a right input. If allow_right_input is set, the caller has to
 * retie that right input of the node (or reinsert the node at a different position where the right input is valid).
 */
template <class NodeType>
void PlanRemoveNode(const std::shared_ptr<AbstractPlanNode<NodeType>>& node,
                    const AllowRightInput allow_right_input = AllowRightInput::kNo) {
  Assert(allow_right_input == AllowRightInput::kYes || !node->RightInput(),
         "Caller did not explicitly confirm that right input should be ignored.");
  /**
   * Back up outputs and in which input side they hold this node
   */
  auto outputs = node->Outputs();
  auto input_sides = node->GetInputSides();

  /**
   * Hold left_input ptr in extra variable to keep the ref count up and untie it from this node.
   * left_input might be nullptr
   */
  auto left_input = node->LeftInput();
  node->SetLeftInput(nullptr);

  /**
   * Tie this node's previous outputs with this nodes previous left input
   * If LeftInput() is nullptr, still call SetInput so this node will get untied from the plan.
   */
  for (size_t i = 0; i < outputs.size(); ++i) {
    outputs[i]->SetInput(input_sides[i], left_input);
  }
}

}  // namespace skyrise
