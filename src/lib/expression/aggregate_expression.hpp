/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"

namespace skyrise {

class AggregateExpression : public AbstractExpression {
 public:
  AggregateExpression(const AggregateFunction aggregate_function, const std::shared_ptr<AbstractExpression>& argument);

  /**
   * @return the argument expression to which the AggregateFunction is applied.
   */
  std::shared_ptr<AbstractExpression> Argument() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;
  AggregateFunction GetAggregateFunction() const;

  /**
   * @return true, if the given @param expression is an AggregateExpression of type COUNT(*).
   */
  static bool IsCountStar(const AbstractExpression& expression);

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;

  const AggregateFunction aggregate_function_;
};

}  // namespace skyrise
