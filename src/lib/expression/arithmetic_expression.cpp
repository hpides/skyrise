/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "arithmetic_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "expression_utils.hpp"
#include "utils/assert.hpp"

namespace skyrise {

std::ostream& operator<<(std::ostream& stream, const ArithmeticOperator arithmetic_operator) {
  switch (arithmetic_operator) {
    case ArithmeticOperator::kAddition:
      stream << "+";
      break;
    case ArithmeticOperator::kSubtraction:
      stream << "-";
      break;
    case ArithmeticOperator::kMultiplication:
      stream << "*";
      break;
    case ArithmeticOperator::kDivision:
      stream << "/";
      break;
    case ArithmeticOperator::kModulo:
      stream << "%";
      break;
  }
  return stream;
}

ArithmeticExpression::ArithmeticExpression(const ArithmeticOperator arithmetic_operator,
                                           std::shared_ptr<AbstractExpression> left_operand,
                                           std::shared_ptr<AbstractExpression> right_operand)
    : AbstractExpression(ExpressionType::kArithmetic, {std::move(left_operand), std::move(right_operand)}),
      arithmetic_operator_(arithmetic_operator) {}

const std::shared_ptr<AbstractExpression>& ArithmeticExpression::LeftOperand() const { return arguments_[0]; }

const std::shared_ptr<AbstractExpression>& ArithmeticExpression::RightOperand() const { return arguments_[1]; }

std::shared_ptr<AbstractExpression> ArithmeticExpression::DeepCopy() const {
  return std::make_shared<ArithmeticExpression>(arithmetic_operator_, LeftOperand()->DeepCopy(),
                                                RightOperand()->DeepCopy());
}

std::string ArithmeticExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;

  stream << EncloseArgument(*LeftOperand(), mode) << " " << arithmetic_operator_ << " "
         << EncloseArgument(*RightOperand(), mode);

  return stream.str();
}

DataType ArithmeticExpression::GetDataType() const {
  return ExpressionCommonType(LeftOperand()->GetDataType(), RightOperand()->GetDataType());
}

ArithmeticOperator ArithmeticExpression::GetArithmeticOperator() const { return arithmetic_operator_; }

bool ArithmeticExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ArithmeticExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  return arithmetic_operator_ == static_cast<const ArithmeticExpression&>(expression).arithmetic_operator_;
}

size_t ArithmeticExpression::ShallowHash() const {
  return boost::hash_value(static_cast<size_t>(arithmetic_operator_));
}

ExpressionPrecedence ArithmeticExpression::Precedence() const {
  switch (arithmetic_operator_) {
    case ArithmeticOperator::kAddition:
    case ArithmeticOperator::kSubtraction:
      return ExpressionPrecedence::kAdditionSubtraction;
    case ArithmeticOperator::kMultiplication:
    case ArithmeticOperator::kDivision:
    case ArithmeticOperator::kModulo:
      return ExpressionPrecedence::kMultiplicationDivision;
  }
  Fail("Invalid enum value");
}

}  // namespace skyrise
