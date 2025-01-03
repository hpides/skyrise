/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "all_type_variant.hpp"
#include "expression_result_views.hpp"
#include "utils/assert.hpp"

namespace skyrise {

class BaseExpressionResult {
 public:
  virtual ~BaseExpressionResult() = default;

  virtual AllTypeVariant ValueAsVariant(const size_t index) const = 0;
};

/**
 * The typed result of a (Sub)Expression.
 * Wraps a vector of values and a vector of NULLs that are filled differently, with the possible combinations.
 *
 * values_
 *      Contains a value for each row if the result is a Series.
 *      Contains a single value if the result is a Literal.
 *
 * nulls_
 *      Is empty if the ExpressionResult is non-nullable.
 *      Contains a bool for each element of values if the ExpressionResult is nullable.
 *      Contains a single element that the determines whether all elements are either NULL or not.
 *
 * Examples:
 *      {values_: [1, 2, 3, 4]; nulls_: []} --> Series [1, 2, 3, 4]
 *      {values_: [1, 2, 3, 4]; nulls_: [false]} --> Series [1, 2, 3, 4]
 *      {values_: [1, 2, 3, 4]; nulls_: [true]} --> Literal [NULL]
 *      {values_: [1, 2, 3, 4]; nulls_: [true, false, true, false]} --> Series [NULL, 2, NULL, 4]
 *      {values_: [1]; nulls_: []} --> Literal [1]
 *      {values_: [1]; nulls_: [true]} --> Literal [NULL]
 *
 * Often the ExpressionEvaluator computes nulls_ and values_ independently, which is why states with redundant
 * information (e.g., {values_: [1, 2, 3, 4]; nulls_: [true]} or {values_: [1, 2, 3, 4]; nulls_: [false]}) are legal.
 */
template <typename T>
class ExpressionResult : public BaseExpressionResult {
 public:
  using Type = T;
  static std::shared_ptr<ExpressionResult<T>> MakeNull() {
    ExpressionResult<T> null_value({{T{}}}, {true});

    return std::make_shared<ExpressionResult<T>>(null_value);
  }

  ExpressionResult() = default;

  explicit ExpressionResult(std::vector<T> values, std::vector<bool> nulls = {})
      : values_(std::move(values)), nulls_(std::move(nulls)) {
    // Allowed size of nulls_: 0 (not nullable)
    //                         1 (nullable, all values_ are NULL or NOT NULL, depending on the value)
    //                         n (same as values_, 1:1 mapping)
    DebugAssert(nulls_.empty() || nulls_.size() == 1 || nulls_.size() == values_.size(),
                "Mismatching number of nulls.");
  }

  bool IsNullableSeries() const { return Size() != 1; }
  bool IsLiteral() const { return Size() == 1; }
  bool IsNullable() const { return !nulls_.empty(); }

  const T& Value(const size_t index) const {
    DebugAssert(Size() == 1 || index < Size(), "Invalid ExpressionResult access.");

    return values_[std::min(index, values_.size() - 1)];
  }

  AllTypeVariant ValueAsVariant(const size_t index) const final {
    return IsNull(index) ? AllTypeVariant{NullValue{}} : AllTypeVariant{Value(index)};
  }

  bool IsNull(const size_t index) const {
    DebugAssert(Size() == 1 || index < Size(), "Null index out of bounds.");

    if (nulls_.empty()) {
      return false;
    }

    return nulls_[std::min(index, nulls_.size() - 1)];
  }

  /**
   * Resolve ExpressionResult<T> to ExpressionResultNullableSeries<T>, ExpressionResultNonNullSeries<T>, or
   * ExpressionResultLiteral<T>.
   *
   * Once resolved, a View does not need to do bounds checking when queried for Value() or IsNull().
   */
  template <typename Functor>
  void AsView(const Functor& functor) const {
    if (Size() == 1) {
      functor(ExpressionResultLiteral(values_.front(), IsNullable() && nulls_.front()));
    } else if (nulls_.size() == 1 && nulls_.front()) {
      functor(ExpressionResultLiteral(T{}, true));
    } else if (!IsNullable()) {
      functor(ExpressionResultNonNullSeries(std::make_shared<std::vector<T>>(values_)));
    } else {
      functor(ExpressionResultNullableSeries(std::make_shared<std::vector<T>>(values_),
                                             std::make_shared<std::vector<bool>>(nulls_)));
    }
  }

  size_t Size() const { return values_.size(); }

  std::vector<T> GetValues() const { return values_; }

  std::vector<bool> GetNulls() const { return nulls_; }

 protected:
  std::vector<T> values_;
  std::vector<bool> nulls_;
};

}  // namespace skyrise
