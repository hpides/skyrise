/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"

namespace skyrise {

/**
 * Unary minus operator to support expressions such as -(a)
 */
class UnaryMinusExpression : public AbstractExpression {
 public:
  explicit UnaryMinusExpression(std::shared_ptr<AbstractExpression> argument);

  std::shared_ptr<AbstractExpression> Argument() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;
};

}  // namespace skyrise
