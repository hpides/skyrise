/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "union_node.hpp"

#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "lqp_expression_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

UnionNode::UnionNode(const SetOperationMode init_set_operation_mode)
    : AbstractLqpNode(LqpNodeType::kUnion), set_operation_mode(init_set_operation_mode) {}

const std::string& UnionNode::Name() const {
  static const std::string kName{"Union"};
  return kName;
}

std::string UnionNode::Description(const DescriptionMode mode,
                                   const AbstractExpression::DescriptionMode /* expression_mode */) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[UnionNode]" << separator;
  stream << "Mode: " << set_operation_mode;
  return stream.str();
}

bool UnionNode::RequiresRightInput() const { return true; }

std::vector<std::shared_ptr<AbstractExpression>> UnionNode::OutputExpressions() const {
  Assert(ExpressionsEqual(LeftInput()->OutputExpressions(), RightInput()->OutputExpressions()),
         "Input Expressions must match");
  return LeftInput()->OutputExpressions();
}

bool UnionNode::IsColumnNullable(const ColumnId column_id) const {
  Assert(LeftInput() && RightInput(), "Need both inputs to determine nullability");

  return LeftInput()->IsColumnNullable(column_id) || RightInput()->IsColumnNullable(column_id);
}

std::shared_ptr<LqpUniqueConstraints> UnionNode::UniqueConstraints() const {
  switch (set_operation_mode) {
    case SetOperationMode::kAll: {
      /**
       * With UnionAll, two tables become merged. The resulting table might contain duplicates.
       * To forward constraints from child nodes, we would have to ensure that both input tables are completely
       * distinct in terms of rows. Currently, there is no strategy. Therefore, we discard all unique constraints.
       */
      return std::make_shared<LqpUniqueConstraints>();
    }
    case SetOperationMode::kUnique:
      Fail("ToDo, see discussion https://github.com/hyrise/hyrise/pull/2156#discussion_r452803825");
  }
  Fail("Unhandled UnionMode");
}

std::vector<FunctionalDependency> UnionNode::NonTrivialFunctionalDependencies() const {
  switch (set_operation_mode) {
    case SetOperationMode::kAll: {
      /**
       * With UnionAll, unique constraints from both input nodes become discarded. To preserve trivial FDs, we
       * request all available FDs from both input nodes.
       */
      const auto& fds_left = LeftInput()->FunctionalDependencies();
      const auto& fds_right = RightInput()->FunctionalDependencies();
      /**
       * Currently, both input tables have the same output expressions for SetOperationMode::kAll. However, the FDs
       * might differ. For example, the left input node could have discarded FDs, whereas the right one has not. To work
       * around this issue, we return the intersected set of FDs which is valid for both input nodes.
       */
      return IntersectFds(fds_left, fds_right);
    }
    default: {
      Fail("Unhandled UnionMode");
    }
  }
}

size_t UnionNode::OnShallowHash() const { return boost::hash_value(set_operation_mode); }

std::shared_ptr<AbstractLqpNode> UnionNode::OnShallowCopy(LqpNodeMapping& /* node_mapping */) const {
  return UnionNode::Make(set_operation_mode);
}

bool UnionNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& /* node_mapping */) const {
  const auto& union_node = static_cast<const UnionNode&>(rhs);
  return set_operation_mode == union_node.set_operation_mode;
}

}  // namespace skyrise
