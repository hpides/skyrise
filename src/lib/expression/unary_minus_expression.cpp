/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "unary_minus_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace skyrise {

UnaryMinusExpression::UnaryMinusExpression(const std::shared_ptr<AbstractExpression>& argument)
    : AbstractExpression(ExpressionType::kUnaryMinus, {argument}) {
  Assert(argument->GetDataType() != DataType::kString, "Cannot negate strings.");
}

std::shared_ptr<AbstractExpression> UnaryMinusExpression::Argument() const { return arguments_[0]; }

std::string UnaryMinusExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "-" << EncloseArgument(*Argument(), mode);
  return stream.str();
}

DataType UnaryMinusExpression::GetDataType() const { return Argument()->GetDataType(); }

bool UnaryMinusExpression::ShallowEquals([[maybe_unused]] const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const UnaryMinusExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return true;
}

std::shared_ptr<AbstractExpression> UnaryMinusExpression::OnDeepCopy() const {
  return std::make_shared<UnaryMinusExpression>(Argument()->DeepCopy());
}

}  // namespace skyrise
