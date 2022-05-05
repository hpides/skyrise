/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_lqp_node.hpp"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <boost/container_hash/hash.hpp>

#include "expression/abstract_expression.hpp"
#include "lqp_expression_utils.hpp"
#include "lqp_utils.hpp"
#include "utils/assert.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

using namespace std::string_literals;  // NOLINT(google-build-using-namespace)

namespace skyrise {

AbstractLqpNode::AbstractLqpNode(LqpNodeType node_type,
                                 const std::vector<std::shared_ptr<AbstractExpression>>& init_node_expressions)
    : type_(node_type), node_expressions_(init_node_expressions) {}

size_t AbstractLqpNode::Hash() const {
  size_t hash{0};

  VisitLqp(SharedFromBase(), [&hash](const auto& node) {
    if (node) {
      for (const auto& expression : node->node_expressions_) {
        boost::hash_combine(hash, expression->Hash());
      }
      boost::hash_combine(hash, node->type_);
      boost::hash_combine(hash, node->OnShallowHash());
      return LqpVisitation::kVisitInputs;
    } else {
      return LqpVisitation::kDoNotVisitInputs;
    }
  });

  return hash;
}

LqpNodeType AbstractLqpNode::Type() const { return type_; }

std::string AbstractLqpNode::Description(const DescriptionMode mode) const {
  return Description(mode, AbstractExpression::DescriptionMode::kColumnName);
}

size_t AbstractLqpNode::OnShallowHash() const { return 0; }

std::shared_ptr<AbstractLqpNode> AbstractLqpNode::DeepCopy(LqpNodeMapping input_node_mapping) const {
  return DeepCopyImpl(input_node_mapping);
}

std::shared_ptr<AbstractLqpNode> AbstractLqpNode::DeepCopyImpl(LqpNodeMapping& node_mapping) const {
  std::shared_ptr<AbstractLqpNode> copied_left_input;
  std::shared_ptr<AbstractLqpNode> copied_right_input;

  if (LeftInput()) {
    copied_left_input = LeftInput()->DeepCopyImpl(node_mapping);
  }
  if (RightInput()) {
    copied_right_input = RightInput()->DeepCopyImpl(node_mapping);
  }

  auto copy = ShallowCopy(node_mapping);
  copy->SetLeftInput(copied_left_input);
  copy->SetRightInput(copied_right_input);

  return copy;
}

bool AbstractLqpNode::ShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const {
  if (type_ != rhs.type_) {
    return false;
  }
  return OnShallowEquals(rhs, node_mapping);
}

std::vector<std::shared_ptr<AbstractExpression>> AbstractLqpNode::OutputExpressions() const {
  Assert(LeftInput() && !RightInput(),
         "Can only forward input expressions iff there is a left input and no right input");
  return LeftInput()->OutputExpressions();
}

std::optional<ColumnId> AbstractLqpNode::FindColumnId(const AbstractExpression& expression) const {
  const auto& output_expressions = this->OutputExpressions();  // Avoid redundant retrieval in loop below
  for (ColumnId column_id = 0; column_id < output_expressions.size(); ++column_id) {
    if (*output_expressions[column_id] == expression) {
      return column_id;
    }
  }
  return std::nullopt;
}

ColumnId AbstractLqpNode::GetColumnId(const AbstractExpression& expression) const {
  const auto column_id = FindColumnId(expression);
  Assert(column_id, "This node has no column '"s + expression.AsColumnName() + "'");
  return *column_id;
}

bool AbstractLqpNode::HasOutputExpressions(const ExpressionUnorderedSet& expressions) const {
  const auto& output_expressions = this->OutputExpressions();

  for (const auto& expression : expressions) {
    if (!std::any_of(output_expressions.cbegin(), output_expressions.cend(),
                     [&expression](const auto& output_expression) { return *output_expression == *expression; })) {
      return false;
    }
  }

  return true;
}

bool AbstractLqpNode::IsColumnNullable(const ColumnId column_id) const {
  // Default behaviour: Forward from input
  Assert(LeftInput() && !RightInput(),
         "Can forward nullability from input iff there is a left input and no right input");
  return LeftInput()->IsColumnNullable(column_id);
}

bool AbstractLqpNode::HasMatchingUniqueConstraint(const ExpressionUnorderedSet& expressions) const {
  DebugAssert(!expressions.empty(), "Invalid input. Set of expressions should not be empty.");
  DebugAssert(HasOutputExpressions(expressions),
              "The given expressions are not a subset of the LQP's output expressions.");

  const auto& unique_constraints = this->UniqueConstraints();
  if (unique_constraints->empty()) {
    return false;
  }

  return ContainsMatchingUniqueConstraint(unique_constraints, expressions);
}

std::vector<FunctionalDependency> AbstractLqpNode::FunctionalDependencies() const {
  // (1) Gather non-trivial FDs and perform sanity checks
  auto non_trivial_fds = NonTrivialFunctionalDependencies();
  if constexpr (SKYRISE_DEBUG) {
    std::unordered_set<FunctionalDependency> fds_set;
    const auto& output_expressions = this->OutputExpressions();
    const auto& output_expressions_set = ExpressionUnorderedSet{output_expressions.cbegin(), output_expressions.cend()};

    for (const auto& fd : non_trivial_fds) {
      auto [_, inserted] = fds_set.insert(fd);
      Assert(inserted, "FDs with the same set of determinant expressions should be merged.");

      for (const auto& fd_determinant_expression : fd.determinant_expressions) {
        // TODO(anyone): C++20: Replace with .contains
        Assert(output_expressions_set.find(fd_determinant_expression) != output_expressions_set.end(),
               "Expected FD's determinant expressions to be a subset of the node's output expressions.");
        Assert(!IsColumnNullable(GetColumnId(*fd_determinant_expression)),
               "Expected FD's determinant expressions to be non-nullable.");
      }
      Assert(std::all_of(fd.dependent_expressions.cbegin(), fd.dependent_expressions.cend(),
                         [&output_expressions_set](const auto& fd_dependent_expression) {
                           // TODO(anyone): C++20: Replace with .contains
                           return output_expressions_set.find(fd_dependent_expression) != output_expressions_set.end();
                         }),
             "Expected the FD's dependent expressions to be a subset of the node's output expressions.");
    }
  }

  // (2) Derive trivial FDs from the node's unique constraints
  const auto& unique_constraints = this->UniqueConstraints();
  // Early exit, if there are no unique constraints
  if (unique_constraints->empty()) {
    return non_trivial_fds;
  }

  auto trivial_fds = FdsFromUniqueConstraints(SharedFromBase(), unique_constraints);

  // (3) Merge and return FDs
  return UnionFds(non_trivial_fds, trivial_fds);
}

std::vector<FunctionalDependency> AbstractLqpNode::NonTrivialFunctionalDependencies() const {
  if (LeftInput()) {
    Assert(!RightInput(), "Expected single input node for implicit FD forwarding. Please override this function.");
    return LeftInput()->NonTrivialFunctionalDependencies();
  } else {
    // e.g. StoredTableNode or StaticTableNode cannot provide any non-trivial FDs
    return {};
  }
}

bool AbstractLqpNode::operator==(const AbstractLqpNode& rhs) const {
  if (this == &rhs) {
    return true;
  }
  return !LqpFindSubplanMismatch(SharedFromBase(), rhs.SharedFromBase());
}

bool AbstractLqpNode::operator!=(const AbstractLqpNode& rhs) const { return !operator==(rhs); }

std::shared_ptr<AbstractLqpNode> AbstractLqpNode::ShallowCopy(LqpNodeMapping& node_mapping) const {
  const auto node_mapping_iter = node_mapping.find(SharedFromBase());

  // Handle diamond shapes in the LQP; don't copy nodes twice
  if (node_mapping_iter != node_mapping.end()) {
    return node_mapping_iter->second;
  }

  auto shallow_copy = OnShallowCopy(node_mapping);
  node_mapping.emplace(SharedFromBase(), shallow_copy);

  return shallow_copy;
}

std::shared_ptr<LqpUniqueConstraints> AbstractLqpNode::ForwardLeftUniqueConstraints() const {
  Assert(LeftInput(), "Cannot forward unique constraints without an input node.");
  const auto& input_unique_constraints = LeftInput()->UniqueConstraints();

  if constexpr (SKYRISE_DEBUG) {
    // Check whether output expressions are missing
    for (const auto& unique_constraint : *input_unique_constraints) {
      Assert(HasOutputExpressions(unique_constraint.expressions),
             "Forwarding of constraints is illegal because node misses output expressions.");
    }
  }
  return input_unique_constraints;
}

std::ostream& operator<<(std::ostream& stream, const AbstractLqpNode& root_node) {
  // Functor returning the inputs of a given node
  const auto get_inputs_fn = [](const auto& node) {
    std::vector<std::shared_ptr<const AbstractLqpNode>> inputs;
    if (node->LeftInput()) {
      inputs.emplace_back(node->LeftInput());
    }
    if (node->RightInput()) {
      inputs.emplace_back(node->RightInput());
    }
    return inputs;
  };

  // Functor writing a given node's description to a given output stream
  const auto node_print_fn = [](const auto& node, auto& output_stream) {
    output_stream << node->Description(DescriptionMode::kSingleLine, AbstractExpression::DescriptionMode::kDetailed);
    if (!node->Comment().empty()) {
      output_stream << " (" << node->Comment() << ")";
    }
    output_stream << " @ " << node;
  };

  PrintDirectedAcyclicGraph<const AbstractLqpNode>(root_node.SharedFromBase(), get_inputs_fn, node_print_fn, stream);

  return stream;
}

}  // namespace skyrise
