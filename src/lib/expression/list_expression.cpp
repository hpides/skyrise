/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "list_expression.hpp"

#include <sstream>

#include "expression_utils.hpp"
#include "utils/assert.hpp"

namespace skyrise {

ListExpression::ListExpression(const std::vector<std::shared_ptr<AbstractExpression>>& elements)
    : AbstractExpression(ExpressionType::kList, elements) {}

const std::vector<std::shared_ptr<AbstractExpression>>& ListExpression::Elements() const { return arguments_; }

std::string ListExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "(";
  stream << ExpressionDescriptions(arguments_, mode);
  stream << ")";
  return stream.str();
}

DataType ListExpression::GetDataType() const {
  Fail("A ListExpression does not have a single type. Each of its elements might have a different type.");
}

bool ListExpression::ShallowEquals([[maybe_unused]] const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ListExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return true;
}

std::shared_ptr<AbstractExpression> ListExpression::OnDeepCopy() const {
  return std::make_shared<ListExpression>(ExpressionsDeepCopy(arguments_));
}

}  // namespace skyrise
