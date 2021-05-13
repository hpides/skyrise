/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_expression.hpp"

#include <queue>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "expression_utils.hpp"
#include "utils/assert.hpp"

namespace skyrise {

AbstractExpression::AbstractExpression(const ExpressionType init_type,
                                       const std::vector<std::shared_ptr<AbstractExpression>>& init_arguments)
    : type(init_type), arguments(init_arguments) {}

std::shared_ptr<AbstractExpression> AbstractExpression::DeepCopy() const { return OnDeepCopy(); }

bool AbstractExpression::RequiresComputation() const { return true; }

bool AbstractExpression::operator==(const AbstractExpression& other) const {
  if (this == &other) {
    return true;
  }

  if (type != other.type) {
    return false;
  }
  if (!ShallowEquals(other)) {
    return false;
  }
  if (!ExpressionsEqual(arguments, other.arguments)) {
    return false;
  }

  return true;
}

bool AbstractExpression::operator!=(const AbstractExpression& other) const { return !operator==(other); }

size_t AbstractExpression::Hash() const {
  size_t hash = boost::hash_value(type);

  for (const auto& argument : arguments) {
    // Include the hash value of the inputs but do not recurse any deeper. A deep comparison is necessary anyway.
    boost::hash_combine(hash, argument->type);
    boost::hash_combine(hash, argument->OnShallowHash());
  }

  boost::hash_combine(hash, OnShallowHash());

  return hash;
}

std::string AbstractExpression::AsColumnName() const { return Description(DescriptionMode::kColumnName); }

size_t AbstractExpression::OnShallowHash() const { return 0; }

ExpressionPrecedence AbstractExpression::Precedence() const { return ExpressionPrecedence::kHighest; }

std::string AbstractExpression::EncloseArgument(const AbstractExpression& argument, const DescriptionMode mode) const {
  // TODO(anybody): Using >= to make divisions ("(2/3)/4") and logical operations ("(a AND (b OR c))") unambiguous.
  //                Sadly this makes cases where the parentheses could be avoided look ugly ("(2+3)+4").

  if (static_cast<std::underlying_type_t<ExpressionPrecedence>>(argument.Precedence()) >=
      static_cast<std::underlying_type_t<ExpressionPrecedence>>(Precedence())) {
    return "(" + argument.Description(mode) + ")";
  } else {
    return argument.Description(mode);
  }
}

}  // namespace skyrise
