/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "dummy_table_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "expression/value_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

DummyTableNode::DummyTableNode() : AbstractLqpNode(LqpNodeType::kDummyTable) {}

const std::string& DummyTableNode::Name() const {
  static const std::string kName{"DummyTable"};
  return kName;
}

std::string DummyTableNode::Description(const DescriptionMode /* mode */,
                                        const AbstractExpression::DescriptionMode /* expression_mode */) const {
  return "[DummyTable]";
}

std::vector<std::shared_ptr<AbstractExpression>> DummyTableNode::OutputExpressions() const { return {}; }

bool DummyTableNode::IsColumnNullable(const ColumnId /* column_id */) const {
  Fail("DummyTable does not output any columns");
}

std::shared_ptr<LqpUniqueConstraints> DummyTableNode::UniqueConstraints() const {
  return std::make_shared<LqpUniqueConstraints>();
}

std::shared_ptr<AbstractLqpNode> DummyTableNode::OnShallowCopy(LqpNodeMapping& /* node_mapping */) const {
  return std::make_shared<DummyTableNode>();
}

bool DummyTableNode::OnShallowEquals(const AbstractLqpNode& /* rhs */, const LqpNodeMapping& /* node_mapping */) const {
  return true;
}

}  // namespace skyrise
