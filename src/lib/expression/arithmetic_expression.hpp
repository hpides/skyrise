/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <iostream>

#include "abstract_expression.hpp"

namespace skyrise {

enum class ArithmeticOperator { kAddition, kSubtraction, kMultiplication, kDivision, kModulo };

std::ostream& operator<<(std::ostream& stream, const ArithmeticOperator arithmetic_operator);

/**
 * ArithmeticExpression represents, e.g. 2 + 3 or 5 * 3 * 4.
 */
class ArithmeticExpression : public AbstractExpression {
 public:
  ArithmeticExpression(const ArithmeticOperator init_arithmetic_operator,
                       const std::shared_ptr<AbstractExpression>& left_operand,
                       const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& LeftOperand() const;
  const std::shared_ptr<AbstractExpression>& RightOperand() const;

  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;

  const ArithmeticOperator arithmetic_operator_;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t OnShallowHash() const override;
  std::shared_ptr<AbstractExpression> OnDeepCopy() const override;
  ExpressionPrecedence Precedence() const override;
};

}  // namespace skyrise
