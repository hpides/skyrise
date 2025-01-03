/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <ostream>

#include "abstract_expression.hpp"

namespace skyrise {

enum class DatetimeComponent { kYear, kMonth, kDay, kHour, kMinute, kSecond };
std::ostream& operator<<(std::ostream& stream, const DatetimeComponent datetime_component);

/**
 * SQL's EXTRACT() function, which provides access to the components of temporal data types.
 */
class ExtractExpression : public AbstractExpression {
 public:
  ExtractExpression(const DatetimeComponent datetime_component, std::shared_ptr<AbstractExpression> from);

  std::shared_ptr<AbstractExpression> From() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;
  DatetimeComponent GetDatetimeComponent() const;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;

  const DatetimeComponent datetime_component_;
};

}  // namespace skyrise
