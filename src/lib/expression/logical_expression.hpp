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
  LogicalExpression(const LogicalOperator init_logical_operator,
                    const std::shared_ptr<AbstractExpression>& left_operand,
                    const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& LeftOperand() const;
  const std::shared_ptr<AbstractExpression>& RightOperand() const;

  std::shared_ptr<AbstractExpression> OnDeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;

  const LogicalOperator logical_operator_;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t OnShallowHash() const override;
  ExpressionPrecedence Precedence() const override;
};

}  // namespace skyrise
