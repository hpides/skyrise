/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_expression.hpp"

#include <queue>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "expression_utils.hpp"

namespace skyrise {

AbstractExpression::AbstractExpression(const ExpressionType type,
                                       std::vector<std::shared_ptr<AbstractExpression>> arguments)
    : type_(type), arguments_(std::move(arguments)) {}

bool AbstractExpression::RequiresComputation() const { return true; }

bool AbstractExpression::operator==(const AbstractExpression& other) const {
  if (this == &other) {
    return true;
  }

  if (type_ != other.type_) {
    return false;
  }
  if (!ShallowEquals(other)) {
    return false;
  }
  if (!ExpressionsEqual(arguments_, other.arguments_)) {
    return false;
  }

  return true;
}

bool AbstractExpression::operator!=(const AbstractExpression& other) const { return !operator==(other); }

size_t AbstractExpression::Hash() const {
  size_t hash = boost::hash_value(type_);

  for (const auto& argument : arguments_) {
    // Include the hash value of the inputs but do not recurse any deeper. A deep comparison is necessary anyway.
    boost::hash_combine(hash, argument->type_);
    boost::hash_combine(hash, argument->ShallowHash());
  }

  boost::hash_combine(hash, ShallowHash());

  return hash;
}

ExpressionType AbstractExpression::GetExpressionType() const { return type_; }

std::vector<std::shared_ptr<AbstractExpression>> AbstractExpression::GetArguments() const { return arguments_; }

std::string AbstractExpression::AsColumnName() const { return Description(DescriptionMode::kColumnName); }

ExpressionPrecedence AbstractExpression::Precedence() const { return ExpressionPrecedence::kHighest; }

std::string AbstractExpression::EncloseArgument(const AbstractExpression& argument, const DescriptionMode mode) const {
  // TODO(tobodner): Using >= to make divisions ("(2/3)/4") and logical operations ("(a AND (b OR c))") unambiguous.
  //                Sadly this makes cases where the parentheses could be avoided look ugly ("(2+3)+4").

  if (static_cast<std::underlying_type_t<ExpressionPrecedence>>(argument.Precedence()) >=
      static_cast<std::underlying_type_t<ExpressionPrecedence>>(Precedence())) {
    return "(" + argument.Description(mode) + ")";
  } else {
    return argument.Description(mode);
  }
}

}  // namespace skyrise
