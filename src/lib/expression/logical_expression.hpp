/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <iostream>

#include "abstract_expression.hpp"

namespace skyrise {

enum class LogicalOperator { kAnd, kOr };
std::ostream& operator<<(std::ostream& stream, const LogicalOperator logical_operator);

class LogicalExpression : public AbstractExpression {
 public:
  LogicalExpression(const LogicalOperator logical_operator, std::shared_ptr<AbstractExpression> left_operand,
                    std::shared_ptr<AbstractExpression> right_operand);

  const std::shared_ptr<AbstractExpression>& LeftOperand() const;
  const std::shared_ptr<AbstractExpression>& RightOperand() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;
  LogicalOperator GetLogicalOperator() const;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;
  ExpressionPrecedence Precedence() const override;

  const LogicalOperator logical_operator_;
};

}  // namespace skyrise
