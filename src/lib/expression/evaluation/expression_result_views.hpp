/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

/**
 * ExpressionResultViews is a concept used internally in the ExpressionEvaluator to allow the compiler to throw
 * away, e.g., calls to IsNull(), if the ExpressionResult is non-nullable and to omit bounds checks in release builds.
 *
 * An ExpressionResult is turned into an ExpressionResultView by calling ExpressionResult::AsView().
 */

/**
 * View that looks at an ExpressionResult knowing that it is a series and may contain NULLs.
 */
template <typename T>
class ExpressionResultNullableSeries {
 public:
  using Type = T;
  ExpressionResultNullableSeries(std::shared_ptr<std::vector<T>> values, std::shared_ptr<std::vector<bool>> nulls)
      : values_(std::move(values)), nulls_(std::move(nulls)) {
    DebugAssert(values_ != nullptr, "Values is nullptr.");
    DebugAssert(nulls_ != nullptr, "Nulls is nullptr.");
    DebugAssert(values_->size() == nulls_->size(), "Need as many values as nulls.");
  }

  bool IsSeries() const { return true; }
  bool IsLiteral() const { return false; }
  bool IsNullable() const { return true; }

  const T& Value(const size_t index) const {
    DebugAssert(index < values_->size(), "Index out of range.");
    return (*values_)[index];
  }

  size_t Size() const { return values_->size(); }

  bool IsNull(const size_t index) const {
    DebugAssert(index < nulls_->size(), "Index out of range.");
    return (*nulls_)[index];
  }

 private:
  std::shared_ptr<std::vector<T>> values_;
  std::shared_ptr<std::vector<bool>> nulls_;
};

/**
 * View that looks at an ExpressionResult knowing that it is a series that does not return NULLs. IsNull() always
 * returns false.
 */
template <typename T>
class ExpressionResultNonNullSeries {
 public:
  using Type = T;
  explicit ExpressionResultNonNullSeries(std::shared_ptr<std::vector<T>> values) : values_(std::move(values)) {
    DebugAssert(values_ != nullptr, "Values is nullptr.");
  }

  bool IsSeries() const { return true; }
  bool IsLiteral() const { return false; }
  bool IsNullable() const { return false; }

  size_t Size() const { return values_->size(); }

  const T& Value(const size_t index) const {
    DebugAssert(index < values_->size(), "Index out of range.");
    return (*values_)[index];
  }

  bool IsNull(const size_t /*index*/) const { return false; }

 private:
  std::shared_ptr<std::vector<T>> values_;
};

/**
 * View that looks at an ExpressionResult knowing that it is a literal. Returns the first element in Value() and
 * IsNull(), no matter which index is requested.
 */
template <typename T>
class ExpressionResultLiteral {
 public:
  using Type = T;
  ExpressionResultLiteral(const T& value, const bool null) : value_(value), null_(null) {}

  bool IsSeries() const { return false; }
  bool IsLiteral() const { return true; }
  bool IsNullable() const { return null_; }

  size_t Size() const { return 1ULL; }

  const T& Value(const size_t /*index*/) const { return value_; }
  bool IsNull(const size_t /*index*/) const { return null_; }

 private:
  T value_;
  bool null_;
};

}  // namespace skyrise
