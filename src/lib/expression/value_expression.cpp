/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "value_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "utils/assert.hpp"

namespace skyrise {

ValueExpression::ValueExpression(const AllTypeVariant& value)
    : AbstractExpression(ExpressionType::kValue, {}), value_(value) {}

std::shared_ptr<AbstractExpression> ValueExpression::DeepCopy() const {
  return std::make_shared<ValueExpression>(value_);
}

bool ValueExpression::RequiresComputation() const { return false; }

std::string ValueExpression::Description(const DescriptionMode /* mode */) const {
  std::stringstream stream;
  stream << value_;
  return stream.str();
}

DataType ValueExpression::GetDataType() const { return DataTypeFromAllTypeVariant(value_); }

AllTypeVariant ValueExpression::GetValue() const { return value_; }

bool ValueExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ValueExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");
  const auto& value_expression =
      static_cast<const ValueExpression&>(expression);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)

  // Even though NULL != NULL, two NULL expressions are the same expressions (e.g., when resolving ColumnIds).
  if (GetDataType() == DataType::kNull && value_expression.GetDataType() == DataType::kNull) {
    return true;
  }

  return value_ == value_expression.value_;
}

size_t ValueExpression::ShallowHash() const { return boost::hash_value(value_); }

}  // namespace skyrise
