#pragma once

#include <sstream>

#include "enable_make_for_plan_node.hpp"
#include "expression/abstract_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

enum class PlanInputSide { kLeft, kRight };

/**
 * AbstractPlanNode is the base class for (logical and physical) query plan nodes.
 *  Each plan node can have a left and a right input node. This class contains basic logic for managing the inputs and
 *  outputs. It provides setter and getter functions for plan nodes, among others.
 */
template <class NodeType>
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class AbstractPlanNode : public std::enable_shared_from_this<AbstractPlanNode<NodeType>>, private Noncopyable {
 public:
  AbstractPlanNode() = default;
  // NOLINTNEXTLINE(bugprone-exception-escape)
  virtual ~AbstractPlanNode() {
    Assert(outputs_.empty(), "There are still outputs referencing this node.");
    if (left_input_) {
      left_input_->RemoveOutputPointer(*this);
    }
    if (right_input_) {
      right_input_->RemoveOutputPointer(*this);
    }
  }

  virtual const std::string& Name() const = 0;

  /**
   * @return a string describing this node's type and properties.
   */
  virtual std::string Description(const DescriptionMode mode) const = 0;

  const std::string& Comment() const { return comment_; }

  void SetComment(std::string comment) { comment_ = std::move(comment); }

  /**
   * Should be overridden in case a node requires two inputs.
   */
  virtual bool RequiresRightInput() const { return false; }

  /**
   * @return the number of inputs set for this node, which is in the interval of [0, 2].
   */
  size_t InputNodeCount() const {
    if (left_input_ && right_input_) {
      return 2;
    } else if (left_input_) {
      return 1;
    }
    return 0;
  }

  /**
   * @return the number of nodes referencing this node as an input, ranging [0, n)
   */
  size_t OutputNodeCount() const { return outputs_.size(); }

  /**
   * Access the inputs/outputs
   *  The outputs are implicitly set and removed in SetLeftInput() / SetRightInput() / SetInput().
   *  SetInput() is a shorthand for SetLeftInput() or SetRightInput(), useful if the side is a runtime value.
   */
  const std::shared_ptr<NodeType>& Input(PlanInputSide input_side) const {
    switch (input_side) {
      case PlanInputSide::kLeft:
        return left_input_;
      case PlanInputSide::kRight:
        DebugAssert(RequiresRightInput(), "This node type does not support a right input.");
        return right_input_;
      default:
        Fail("Unhandled PlanInputSide.");
    }
  }

  std::vector<std::shared_ptr<NodeType>> Inputs() const {
    std::vector<std::shared_ptr<NodeType>> inputs;
    inputs.reserve(InputNodeCount());
    if (LeftInput()) {
      inputs.emplace_back(LeftInput());
      if (RightInput()) {
        inputs.emplace_back(RightInput());
      }
    }
    return inputs;
  }

  const std::shared_ptr<NodeType>& LeftInput() const { return left_input_; }

  const std::shared_ptr<NodeType>& RightInput() const { return right_input_; }

  void SetLeftInput(const std::shared_ptr<NodeType>& left_input) { SetInput(PlanInputSide::kLeft, left_input); }

  void SetRightInput(const std::shared_ptr<NodeType>& right_input) { SetInput(PlanInputSide::kRight, right_input); }

  void SetInput(PlanInputSide input_side, const std::shared_ptr<NodeType>& input) {
    DebugAssert(input_side == PlanInputSide::kLeft || input == nullptr || RequiresRightInput(),
                "The plan node " + Name() + " does not accept a right input");
    auto& current_input = input_side == PlanInputSide::kLeft ? left_input_ : right_input_;
    if (current_input == input) {
      return;
    }

    // Untie from previous input.
    if (current_input) {
      current_input->RemoveOutputPointer(*this);
    }

    // Tie in the new input.
    current_input = input;
    if (current_input) {
      current_input->AddOutputPointer(SharedFromBase());
    }
  }

  /**
   * @pre this has @param output as an output.
   * @return whether this is the left or right input in the specified output.
   */
  PlanInputSide GetInputSide(const std::shared_ptr<NodeType>& output) const {
    if (output->left_input_.Get() == this) {
      return PlanInputSide::kLeft;
    } else if (output->right_input_.Get() == this) {
      return PlanInputSide::kRight;
    }
    Fail("Undefined PlanInputSide because specified output node is not actually an output node of this node.");
  }

  /**
   * @return {get_output_side(outputs()[0]), ..., get_output_side(outputs()[n-1])}
   */
  std::vector<PlanInputSide> GetInputSides() const {
    std::vector<PlanInputSide> input_sides;
    input_sides.reserve(outputs_.size());

    for (const auto& weak_output : outputs_) {
      const auto output = weak_output.lock();
      DebugAssert(output, "Failed to lock output.");
      input_sides.emplace_back(GetInputSide(output));
    }

    return input_sides;
  }

  /**
   * Locks all outputs (as they are stored with std::weak_ptr) and returns them as std::shared_ptr.
   */
  std::vector<std::shared_ptr<NodeType>> Outputs() const {
    std::vector<std::shared_ptr<NodeType>> outputs;
    outputs.reserve(outputs_.size());

    for (const auto& output_weak_ptr : outputs_) {
      const auto output = output_weak_ptr.lock();
      DebugAssert(output, "Failed to lock output.");
      outputs.emplace_back(output);
    }

    return outputs;
  }

  /**
   * WORKAROUND
   *  Always use SharedFromBase() instead of shared_from_this(). This workaround is needed to provide the functionality
   *  in the base class as well as in derived classes. (https://stackoverflow.com/a/32172486/5558040)
   */
  std::shared_ptr<NodeType> SharedFromBase() { return std::static_pointer_cast<NodeType>(this->shared_from_this()); }

  std::shared_ptr<const NodeType> SharedFromBase() const {
    return std::static_pointer_cast<const NodeType>(this->shared_from_this());
  }

 protected:
  /**
   * Holds a (short) comment that can be added to the node description or printed during plan visualization.
   */
  std::string comment_;

 private:
  std::shared_ptr<NodeType> left_input_;
  std::shared_ptr<NodeType> right_input_;
  std::vector<std::weak_ptr<NodeType>> outputs_;

  /**
   * For internal usage in SetLeftInput(), SetRightInput(), SetInput().
   * Add or remove an output without manipulating this output's input ptr.
   */
  void AddOutputPointer(const std::shared_ptr<NodeType>& output) { outputs_.emplace_back(output); }

  void RemoveOutputPointer(const AbstractPlanNode<NodeType>& output) {
    const auto iter = std::find_if(outputs_.cbegin(), outputs_.cend(), [&](const auto& other) {
      /**
       * Workaround
       *  Normally we'd just check `&output == other.lock().Get()` here.
       *  BUT (this is the hacky part), we're checking for `other.expired()` here as well and accept an expired element
       *  as a match. If nothing else breaks the only way we might get an expired element is if `other` is the expired
       *  weak_ptr<> to `output` - and thus the element we're looking for - in the following scenario:
       *
       *    auto node_a = Node::Make()
       *    auto node_b = Node::Make(..., node_a)
       *
       *    node_b.reset(); // node_b::~AbstractPlanNode() will call `node_a.RemoveOutputPointer(node_b)`
       *                    // But we can't lock node_b anymore, since its ref count is already 0
       */
      return &output == other.lock().get() || other.expired();
    });
    Assert(iter != outputs_.end(), "Specified output node is not actually an output of this node.");
    outputs_.erase(iter);
  }
};

}  // namespace skyrise
