/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "logical_plan_root_node.hpp"

#include <string>

#include "abstract_lqp_node.hpp"

namespace skyrise {

LogicalPlanRootNode::LogicalPlanRootNode() : AbstractLqpNode(LqpNodeType::kRoot) {}

const std::string& LogicalPlanRootNode::Name() const {
  static const std::string kName = "LogicalPlanRoot";
  return kName;
}

std::string LogicalPlanRootNode::Description(const DescriptionMode /* mode */,
                                             const AbstractExpression::DescriptionMode /* expression_mode */) const {
  return "[LogicalPlanRoot]";
}

std::shared_ptr<AbstractLqpNode> LogicalPlanRootNode::OnShallowCopy(LqpNodeMapping& /* node_mapping */) const {
  return Make();
}

std::shared_ptr<LqpUniqueConstraints> LogicalPlanRootNode::UniqueConstraints() const {
  Fail("LogicalPlanRootNode is not expected to be queried for unique constraints.");
}

std::vector<FunctionalDependency> LogicalPlanRootNode::NonTrivialFunctionalDependencies() const {
  Fail("LogicalPlanRootNode is not expected to be queried for functional dependencies.");
}

bool LogicalPlanRootNode::OnShallowEquals(const AbstractLqpNode& /* rhs */,
                                          const LqpNodeMapping& /* node_mapping */) const {
  return true;
}

}  // namespace skyrise
