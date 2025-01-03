/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <optional>

#include <boost/variant.hpp>

#include "abstract_expression.hpp"
#include "operator/abstract_operator.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * SQL CASE. To build a case expression with multiple WHEN clauses, nest additional CaseExpressions into the otherwise
 * branch.
 */
class CaseExpression : public AbstractExpression {
 public:
  CaseExpression(const std::shared_ptr<AbstractExpression>& when, const std::shared_ptr<AbstractExpression>& then,
                 const std::shared_ptr<AbstractExpression>& otherwise);

  const std::shared_ptr<AbstractExpression>& When() const;
  const std::shared_ptr<AbstractExpression>& Then() const;
  const std::shared_ptr<AbstractExpression>& Otherwise() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;

  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;
};

}  // namespace skyrise
