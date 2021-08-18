/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"

namespace skyrise {

/**
 * SQL's LIST used as the right operand of IN.
 */
class ListExpression : public AbstractExpression {
 public:
  explicit ListExpression(std::vector<std::shared_ptr<AbstractExpression>> elements);

  const std::vector<std::shared_ptr<AbstractExpression>>& Elements() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;
};

}  // namespace skyrise
