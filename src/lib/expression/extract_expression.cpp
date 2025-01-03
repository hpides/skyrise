/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "extract_expression.hpp"

#include <sstream>

namespace skyrise {

std::ostream& operator<<(std::ostream& stream, const DatetimeComponent datetime_component) {
  switch (datetime_component) {
    case DatetimeComponent::kYear:
      stream << "YEAR";
      break;
    case DatetimeComponent::kMonth:
      stream << "MONTH";
      break;
    case DatetimeComponent::kDay:
      stream << "DAY";
      break;
    case DatetimeComponent::kHour:
      stream << "HOUR";
      break;
    case DatetimeComponent::kMinute:
      stream << "MINUTE";
      break;
    case DatetimeComponent::kSecond:
      stream << "SECOND";
      break;
    default:
      Fail("Unsupported DatetimeComponent type.");
  }
  return stream;
}

ExtractExpression::ExtractExpression(const DatetimeComponent datetime_component,
                                     std::shared_ptr<AbstractExpression> from)
    : AbstractExpression(ExpressionType::kExtract, {std::move(from)}), datetime_component_(datetime_component) {}

std::shared_ptr<AbstractExpression> ExtractExpression::From() const { return arguments_[0]; }

std::shared_ptr<AbstractExpression> ExtractExpression::DeepCopy() const {
  return std::make_shared<ExtractExpression>(datetime_component_, From()->DeepCopy());
}

std::string ExtractExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "EXTRACT(" << datetime_component_ << " FROM " << From()->Description(mode) << ")";
  return stream.str();
}

DataType ExtractExpression::GetDataType() const {
  // Dates are kString, DateComponents could be Ints, but lets leave it at String for now.
  // TODO(tobodner): Revisit DataType once the ExpressionEvaluator is introduced.
  return DataType::kString;
}

DatetimeComponent ExtractExpression::GetDatetimeComponent() const { return datetime_component_; }

bool ExtractExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ExtractExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");
  const auto& other_extract_expression =
      static_cast<const ExtractExpression&>(expression);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)

  return other_extract_expression.datetime_component_ == datetime_component_;
}

size_t ExtractExpression::ShallowHash() const {
  using DatetimeUnderlyingType = std::underlying_type_t<DatetimeComponent>;
  return std::hash<DatetimeUnderlyingType>{}(static_cast<DatetimeUnderlyingType>(datetime_component_));
}

}  // namespace skyrise
