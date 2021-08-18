/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "unary_minus_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace skyrise {

UnaryMinusExpression::UnaryMinusExpression(std::shared_ptr<AbstractExpression> argument)
    : AbstractExpression(ExpressionType::kUnaryMinus, {std::move(argument)}) {
  Assert(arguments_[0]->GetDataType() != DataType::kString, "Cannot negate strings.");
}

std::shared_ptr<AbstractExpression> UnaryMinusExpression::Argument() const { return arguments_[0]; }

std::shared_ptr<AbstractExpression> UnaryMinusExpression::DeepCopy() const {
  return std::make_shared<UnaryMinusExpression>(Argument()->DeepCopy());
}

std::string UnaryMinusExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "-" << EncloseArgument(*Argument(), mode);
  return stream.str();
}

DataType UnaryMinusExpression::GetDataType() const { return Argument()->GetDataType(); }

bool UnaryMinusExpression::ShallowEquals([[maybe_unused]] const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const UnaryMinusExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");
  return true;
}

size_t UnaryMinusExpression::ShallowHash() const {
  // UnaryMinusExpression introduces no additional data fields. Therefore, we return a constant hash value.
  return 0;
}

}  // namespace skyrise
