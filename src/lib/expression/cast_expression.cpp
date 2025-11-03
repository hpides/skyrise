/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "cast_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

namespace skyrise {

CastExpression::CastExpression(std::shared_ptr<AbstractExpression> argument, const DataType data_type)
    : AbstractExpression(ExpressionType::kCast, {std::move(argument)}), data_type_(data_type) {}

std::shared_ptr<AbstractExpression> CastExpression::Argument() const { return arguments_[0]; }

std::shared_ptr<AbstractExpression> CastExpression::DeepCopy() const {
  return std::make_shared<CastExpression>(Argument()->DeepCopy(), data_type_);
}

std::string CastExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "CAST(" << Argument()->Description(mode) << " AS " << data_type_ << ")";
  return stream.str();
}

DataType CastExpression::GetDataType() const { return data_type_; }

bool CastExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const CastExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  const auto& other_cast_expression = static_cast<const CastExpression&>(expression);
  return data_type_ == other_cast_expression.data_type_;
}

size_t CastExpression::ShallowHash() const { return boost::hash_value(static_cast<size_t>(data_type_)); }

}  // namespace skyrise
