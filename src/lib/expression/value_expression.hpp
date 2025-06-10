/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace skyrise {

/**
 * Wraps an AllTypeVariant
 */
class ValueExpression : public AbstractExpression {
 public:
  explicit ValueExpression(const AllTypeVariant& value);

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  bool RequiresComputation() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;
  AllTypeVariant GetValue() const;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;

  const AllTypeVariant value_;
};

}  // namespace skyrise
