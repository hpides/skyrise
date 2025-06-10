/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>

#include "abstract_expression.hpp"
#include "aggregate_expression.hpp"
#include "all_type_variant.hpp"
#include "arithmetic_expression.hpp"
#include "between_expression.hpp"
#include "binary_predicate_expression.hpp"
#include "case_expression.hpp"
#include "cast_expression.hpp"
#include "extract_expression.hpp"
#include "in_expression.hpp"
#include "is_null_expression.hpp"
#include "list_expression.hpp"
#include "logical_expression.hpp"
#include "pqp_column_expression.hpp"
#include "types.hpp"
#include "unary_minus_expression.hpp"
#include "value_expression.hpp"

/**
 * This file provides convenience methods to create (nested) Expression objects with little boilerplate code.
 *
 * Note that functions suffixed with "_" (e.g., Equals_()) are not ordinary functions.
 *
 * Notation:
 *      And_(Equals_(1, 1), Equals_(2,2))
 *
 * Meaning:
 *     const auto value_1 = std::make_shared<ValueExpression>(1);
 *     const auto value_2 = std::make_shared<ValueExpression>(2);
 *     const auto 1_eq_1 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::kEquals, value_1, value_1);
 *     const auto 2_eq_2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::kEquals, value_2, value_2);
 *     const auto logical_expression = std::make_shared<LogicalExpression>(LogicalOperator::kAnd, 1_eq_1, 2_eq_2);
 */

namespace skyrise {

/**
 * Named expression_"functional" since it supplies a functional-programming-like interface to build nested expressions.
 */
namespace expression_functional {

/**
 * @defgroup Turn expression-like things (e.g., Values or Expressions) into expressions.
 *
 * Mostly used internally in this file.
 * @{
 */
std::shared_ptr<AbstractExpression> ToExpression(const std::shared_ptr<AbstractExpression>& expression);
std::shared_ptr<ValueExpression> ToExpression(const AllTypeVariant& value);
/** @} */

// NOLINTNEXTLINE(readability-identifier-naming)
std::shared_ptr<ValueExpression> Value_(const AllTypeVariant& value);
// NOLINTNEXTLINE(readability-identifier-naming)
std::shared_ptr<ValueExpression> Null_();

namespace detail {

/**
 * @defgroup Static objects that create Expressions that have an enum member (e.g., PredicateCondition::kEquals).
 *
 * Having these eliminates the need to specify a function for each Expression-Enum-Member combination.
 * @{
 */
template <auto T, typename E>
struct Unary final {
  template <typename A>
  std::shared_ptr<E> operator()(const A& a) const {
    return std::make_shared<E>(T, ToExpression(a));
  }
};

template <auto T, typename E>
struct Binary final {
  template <typename A, typename B>
  std::shared_ptr<E> operator()(const A& a, const B& b) const {
    return std::make_shared<E>(T, ToExpression(a), ToExpression(b));
  }
};

template <auto T, typename E>
struct Ternary final {
  template <typename A, typename B, typename C>
  std::shared_ptr<E> operator()(const A& a, const B& b, const C& c) const {
    return std::make_shared<E>(T, ToExpression(a), ToExpression(b), ToExpression(c));
  }
};
/** @} */

}  // namespace detail

// NOLINTBEGIN(readability-identifier-naming)

inline detail::Unary<PredicateCondition::kIsNull, IsNullExpression> IsNull_;
inline detail::Unary<PredicateCondition::kIsNotNull, IsNullExpression> IsNotNull_;
inline detail::Unary<AggregateFunction::kSum, AggregateExpression> Sum_;
inline detail::Unary<AggregateFunction::kMax, AggregateExpression> Max_;
inline detail::Unary<AggregateFunction::kMin, AggregateExpression> Min_;
inline detail::Unary<AggregateFunction::kAvg, AggregateExpression> Avg_;
inline detail::Unary<AggregateFunction::kCount, AggregateExpression> Count_;
inline detail::Unary<AggregateFunction::kCountDistinct, AggregateExpression> CountDistinct_;
inline detail::Unary<AggregateFunction::kStringAgg, AggregateExpression> String_Agg_;

inline detail::Binary<ArithmeticOperator::kDivision, ArithmeticExpression> Div_;
inline detail::Binary<ArithmeticOperator::kMultiplication, ArithmeticExpression> Mul_;
inline detail::Binary<ArithmeticOperator::kAddition, ArithmeticExpression> Add_;
inline detail::Binary<ArithmeticOperator::kSubtraction, ArithmeticExpression> Sub_;
inline detail::Binary<ArithmeticOperator::kModulo, ArithmeticExpression> Mod_;
inline detail::Binary<PredicateCondition::kEquals, BinaryPredicateExpression> Equals_;
inline detail::Binary<PredicateCondition::kNotEquals, BinaryPredicateExpression> NotEquals_;
inline detail::Binary<PredicateCondition::kLessThan, BinaryPredicateExpression> LessThan_;
inline detail::Binary<PredicateCondition::kLessThanEquals, BinaryPredicateExpression> LessThanEquals_;
inline detail::Binary<PredicateCondition::kGreaterThanEquals, BinaryPredicateExpression> GreaterThanEquals_;
inline detail::Binary<PredicateCondition::kGreaterThan, BinaryPredicateExpression> GreaterThan_;
inline detail::Binary<LogicalOperator::kAnd, LogicalExpression> And_;
inline detail::Binary<LogicalOperator::kOr, LogicalExpression> Or_;

inline detail::Ternary<PredicateCondition::kBetweenInclusive, BetweenExpression> BetweenInclusive_;
inline detail::Ternary<PredicateCondition::kBetweenLowerExclusive, BetweenExpression> BetweenLowerExclusive_;
inline detail::Ternary<PredicateCondition::kBetweenUpperExclusive, BetweenExpression> BetweenUpperExclusive_;
inline detail::Ternary<PredicateCondition::kBetweenExclusive, BetweenExpression> BetweenExclusive_;

inline detail::Unary<AggregateFunction::kAny, AggregateExpression> Any_;
inline detail::Unary<AggregateFunction::kStandardDeviationSample, AggregateExpression> StandardDeviationSample_;

template <typename A, typename B, typename C>
std::shared_ptr<CaseExpression> Case_(const A& when, const B& then, const C& otherwise) {
  return std::make_shared<CaseExpression>(ToExpression(when), ToExpression(then), ToExpression(otherwise));
}

template <typename Argument>
std::shared_ptr<CastExpression> Cast_(const Argument& argument, const DataType data_type) {
  return std::make_shared<CastExpression>(ToExpression(argument), data_type);
}

template <typename... Args>
std::vector<std::shared_ptr<AbstractExpression>> ExpressionVector_(Args&&... args) {
  return std::vector<std::shared_ptr<AbstractExpression>>({ToExpression(std::forward<Args>(args))...});
}

template <typename F>
std::shared_ptr<ExtractExpression> Extract_(const DatetimeComponent datetime_component, const F& from) {
  return std::make_shared<ExtractExpression>(datetime_component, ToExpression(from));
}

template <typename V, typename S>
std::shared_ptr<InExpression> In_(const V& v, const S& s) {
  return std::make_shared<InExpression>(PredicateCondition::kIn, ToExpression(v), ToExpression(s));
}

template <typename... Args>
std::shared_ptr<ListExpression> List_(Args&&... args) {
  return std::make_shared<ListExpression>(ExpressionVector_(std::forward<Args>(args)...));
}

template <typename V, typename S>
std::shared_ptr<InExpression> NotIn_(const V& v, const S& s) {
  return std::make_shared<InExpression>(PredicateCondition::kNotIn, ToExpression(v), ToExpression(s));
}

std::shared_ptr<PqpColumnExpression> PqpColumn_(const ColumnId column_id, const DataType data_type, const bool nullable,
                                                const std::string& column_name);

template <typename Argument>
std::shared_ptr<UnaryMinusExpression> UnaryMinus_(const Argument& argument) {
  return std::make_shared<UnaryMinusExpression>(ToExpression(argument));
}

// NOLINTEND(readability-identifier-naming)

}  // namespace expression_functional

}  // namespace skyrise
