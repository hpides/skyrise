/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>

#include "abstract_expression.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * SQL's CAST function.
 */
class CastExpression : public AbstractExpression {
 public:
  CastExpression(std::shared_ptr<AbstractExpression> argument, DataType data_type);

  std::shared_ptr<AbstractExpression> Argument() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;

  const DataType data_type_;
};

}  // namespace skyrise
