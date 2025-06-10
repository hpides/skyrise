#pragma once

#include "abstract_plan_node.hpp"
#include "utils/assert.hpp"

namespace skyrise {

enum class AllowRightInput { kNo, kYes };

template <class NodeType>
void ReplacePlanNode(const std::shared_ptr<AbstractPlanNode<NodeType>>& original_node,
                     const std::shared_ptr<AbstractPlanNode<NodeType>>& replacement_node) {
  Assert(!replacement_node->LeftInput(), "Expected node without left input.");
  Assert(!replacement_node->RightInput(), "Expected node without right input.");
  Assert(replacement_node->OutputNodeCount() == 0, "Expected node without outputs.");

  const auto outputs = original_node->Outputs();
  const auto input_sides = original_node->GetInputSides();

  // Tie the replacement_node with this node's inputs
  replacement_node->SetLeftInput(original_node->LeftInput());
  replacement_node->SetRightInput(original_node->RightInput());

  // Tie the replacement_node with this node's outputs.
  for (size_t i = 0; i < outputs.size(); ++i) {
    outputs[i]->SetInput(input_sides[i], std::static_pointer_cast<NodeType>(replacement_node));
  }

  // Untie this node from the plan
  original_node->SetLeftInput(nullptr);
  original_node->SetRightInput(nullptr);
}

template <class NodeType>
void InsertPlanNodeAbove(const std::shared_ptr<AbstractPlanNode<NodeType>>& node,
                         const std::shared_ptr<AbstractPlanNode<NodeType>>& node_to_insert,
                         const AllowRightInput allow_right_input = AllowRightInput::kNo) {
  Assert(!node_to_insert->LeftInput(), "Expected node without left input.");
  Assert(!node_to_insert->RightInput() || allow_right_input == AllowRightInput::kYes,
         "Expected node without right input.");
  Assert(node_to_insert->OutputNodeCount() == 0, "Expected node without outputs.");

  // Re-Link @param node's outputs to @param node_to_insert
  const auto node_outputs = node->Outputs();
  for (const auto& node_output : node_outputs) {
    const PlanInputSide input_side = node->GetInputSide(node_output);
    node_output->SetInput(input_side, std::static_pointer_cast<NodeType>(node_to_insert));
  }

  // Place @param node_to_insert above @param node
  node_to_insert->SetLeftInput(std::static_pointer_cast<NodeType>(node));
}

template <class NodeType>
void InsertPlanNodeBelow(const std::shared_ptr<AbstractPlanNode<NodeType>>& parent_node, const PlanInputSide input_side,
                         const std::shared_ptr<AbstractPlanNode<NodeType>>& node_to_insert,
                         const AllowRightInput allow_right_input = AllowRightInput::kNo) {
  Assert(!node_to_insert->LeftInput(), "Expected node without left input.");
  Assert(!node_to_insert->RightInput() || allow_right_input == AllowRightInput::kYes,
         "Expected node without right input.");
  Assert(node_to_insert->OutputNodeCount() == 0, "Expected node without outputs.");

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
void RemovePlanNode(const std::shared_ptr<AbstractPlanNode<NodeType>>& node,
                    const AllowRightInput allow_right_input = AllowRightInput::kNo) {
  Assert(allow_right_input == AllowRightInput::kYes || !node->RightInput(),
         "Caller did not explicitly confirm that right input should be ignored.");

  // Back up outputs and in which input side they hold this node.
  auto outputs = node->Outputs();
  auto input_sides = node->GetInputSides();

  // Hold left_input in an extra variable to keep the reference count up and untie it from this node.
  auto left_input = node->LeftInput();
  node->SetLeftInput(nullptr);

  // Tie this node's previous outputs with this node's previous left input. If left_input is a nullptr, still call
  // SetInput so this node will get untied from the plan.
  for (size_t i = 0; i < outputs.size(); ++i) {
    outputs[i]->SetInput(input_sides[i], left_input);
  }
}

}  // namespace skyrise
