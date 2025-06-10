/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "all_type_variant.hpp"
#include "expression_precedence.hpp"
#include "types.hpp"

namespace skyrise {

enum class ExpressionType : uint8_t {
  kAggregate,
  kArithmetic,
  kCase,
  kCast,
  kExtract,
  kList,
  kLogical,
  kPqpColumn,
  kPredicate,
  kUnaryMinus,
  kValue
};

/**
 * AbstractExpression is a self-contained data structure describing Expressions.
 *
 * Expressions are Literals, Columns, Arithmetics (a + b, ...), Logicals (a AND b), Lists (`('a', 'b')`), and
 * Subqueries. Check out the classes derived from AbstractExpression for all available types.
 *
 * Expressions are evaluated (typically for all rows of a Chunk) using the ExpressionEvaluator.
 */
class AbstractExpression : public std::enable_shared_from_this<AbstractExpression>, private Noncopyable {
 public:
  AbstractExpression(const ExpressionType type, std::vector<std::shared_ptr<AbstractExpression>> arguments);
  virtual ~AbstractExpression() = default;

  /**
   * Recursively check for Expression equality.
   * @pre Both expressions need to reference the same LQP
   */
  bool operator==(const AbstractExpression& other) const;
  bool operator!=(const AbstractExpression& other) const;

  /**
   * @return A deep copy of the expression.
   */
  virtual std::shared_ptr<AbstractExpression> DeepCopy() const = 0;

  /**
   * Certain expression types (Parameters, Literals, and Columns) do not require computation and therefore do not
   * require temporary columns with their result in them.
   */
  virtual bool RequiresComputation() const;

  /**
   * @return The expression's column name or, optionally, a more detailed description of the expression
   */
  enum class DescriptionMode : uint8_t {
    kColumnName,  // Returns only the column name
    kDetailed     // Additionally includes the address of referenced nodes
  };
  virtual std::string Description(const DescriptionMode mode = DescriptionMode::kDetailed) const = 0;

  /**
   * @return A human readable string representing the Expression that can be used as a column name
   *          (shortcut for Description(DescriptionMode::kColumnName))
   */
  std::string AsColumnName() const;

  /**
   * @return The DataType of the result of the expression
   */
  virtual DataType GetDataType() const = 0;

  size_t Hash() const;

  ExpressionType GetExpressionType() const;

  std::vector<std::shared_ptr<AbstractExpression>> GetArguments() const;

 protected:
  /**
   * Override to check data fields for equality in derived types.
   */
  virtual bool ShallowEquals(const AbstractExpression& expression) const = 0;

  /**
   * Override to hash data fields in derived types.
   */
  virtual size_t ShallowHash() const = 0;

  /**
   * Used internally in EncloseArgument() to put parentheses around expression arguments if they have a lower
   * precedence than the expression itself.
   * Lower precedence indicates tighter binding, compare https://en.cppreference.com/w/cpp/language/operator_precedence.
   *
   * @return 0 by default
   */
  virtual ExpressionPrecedence Precedence() const;

  /**
   * @return The argument.Description(mode), enclosed by parentheses if the argument precedence is lower than
   *          this->Precedence()
   */
  std::string EncloseArgument(const AbstractExpression& argument, const DescriptionMode mode) const;

  const ExpressionType type_;
  std::vector<std::shared_ptr<AbstractExpression>> arguments_;
};

/**
 * Used for printing readable error messages.
 */
inline std::ostream& operator<<(std::ostream& stream, const AbstractExpression& expression) {
  return stream << expression.Description();
}

/**
 * Wrapper around expression->Hash(), to enable hash-based containers containing std::shared_ptr<AbstractExpression>.
 */
struct ExpressionSharedPtrHash final {
  size_t operator()(const std::shared_ptr<AbstractExpression>& expression) const { return expression->Hash(); }
  size_t operator()(const std::shared_ptr<const AbstractExpression>& expression) const { return expression->Hash(); }
};

/**
 * Wrapper around AbstractExpression::operator==() to enable hash-based containers containing
 * std::shared_ptr<AbstractExpression>.
 */
struct ExpressionSharedPtrEqual final {
  size_t operator()(const std::shared_ptr<const AbstractExpression>& expression_a,
                    const std::shared_ptr<const AbstractExpression>& expression_b) const {
    return *expression_a == *expression_b;
  }
  size_t operator()(const std::shared_ptr<AbstractExpression>& expression_a,
                    const std::shared_ptr<AbstractExpression>& expression_b) const {
    return *expression_a == *expression_b;
  }
};

/**
 * Warning regarding the following unordered associative containers:
 *  operator== ignores the equality functions, see
 *  https://stackoverflow.com/questions/36167764/can-not-compare-stdunorded-set-with-custom-keyequal
 */
template <typename Value>
using ConstExpressionUnorderedMap = std::unordered_map<std::shared_ptr<const AbstractExpression>, Value,
                                                       ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;
using ExpressionUnorderedSet =
    std::unordered_set<std::shared_ptr<AbstractExpression>, ExpressionSharedPtrHash, ExpressionSharedPtrEqual>;

}  // namespace skyrise
