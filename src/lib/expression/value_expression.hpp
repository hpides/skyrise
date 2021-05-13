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
  explicit ValueExpression(const AllTypeVariant& init_value);

  bool RequiresComputation() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;

  const AllTypeVariant value_;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t OnShallowHash() const override;
  std::shared_ptr<AbstractExpression> OnDeepCopy() const override;
};

}  // namespace skyrise
