/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cmath>

#include "all_type_variant.hpp"
#include "expression_result.hpp"

/**
 * Internal functor objects for the ExpressionEvaluator.
 */
namespace skyrise {

/**
 * Indicates whether T is a valid argument type to a logical expression.
 */
template <typename T>
constexpr bool kIsLogicalOperand = std::is_same_v<int32_t, T> || std::is_same_v<NullValue, T>;

/**
 * Turn a bool into itself and a NULL into false.
 */
// BEGINNOLINT(misc-definitions-in-headers)
bool inline ToBool(const bool value) { return value; }
bool inline ToBool(const NullValue& /*value*/) { return false; }
// ENDNOLINT(misc-definitions-in-headers)

/**
 * Cast a value into another type.
 */
template <typename T, typename V>
T ToValue(const V& value) {
  return static_cast<T>(value);
}

/**
 * Cast a NULL into another type.
 */
template <typename T>
T ToValue(const NullValue& /*value*/) {
  return T{};
}

/**
 * SQL's OR with a ternary NULL logic (i.e., TRUE OR NULL -> TRUE).
 */
struct TernaryOrEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct Supports {
    static constexpr bool kValue = kIsLogicalOperand<Result> && kIsLogicalOperand<ArgA> && kIsLogicalOperand<ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result_value, bool& result_null, const ArgA& a_value, const bool a_null, const ArgB& b_value,
                  const bool b_null) {
    const bool a_is_true = !a_null && ToBool(a_value);
    const bool b_is_true = !b_null && ToBool(b_value);

    result_value = a_is_true || b_is_true;
    result_null = (a_null || b_null) && !result_value;
  }
};

/**
 * SQL's AND with a ternary NULL logic (i.e., FALSE AND NULL -> FALSE).
 */
struct TernaryAndEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct Supports {
    static constexpr bool kValue = kIsLogicalOperand<Result> && kIsLogicalOperand<ArgA> && kIsLogicalOperand<ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result_value, bool& result_null, const ArgA& a_value, const bool a_null, const ArgB& b_value,
                  const bool b_null) {
    const bool a_is_true = !a_null && ToBool(a_value);
    const bool b_is_true = !b_null && ToBool(b_value);

    result_value = a_is_true && b_is_true;
    result_null = a_null && b_null;

    if constexpr (!std::is_same_v<NullValue, ArgA>) {
      result_null |= a_value && b_null;
    }
    if constexpr (!std::is_same_v<NullValue, ArgB>) {
      result_null |= b_value && a_null;
    }
  }
};

/**
 * Wrap a STL comparison operator (e.g., std::equal_to) so that it exposes a Supports::value member and supports a
 * NullValue as argument.
 */
template <template <typename T> typename Functor>
struct StlComparisonFunctorWrapper {
  template <typename Result, typename ArgA, typename ArgB>
  struct Supports {
    static constexpr bool kValue =
        std::is_same_v<int32_t, Result> &&
        (!std::is_same_v<std::string, ArgA> ||
         (std::is_same_v<NullValue, ArgB> || std::is_same_v<std::string, ArgB>)) &&
        (!std::is_same_v<std::string, ArgB> || (std::is_same_v<NullValue, ArgA> || std::is_same_v<std::string, ArgA>));
  };

  template <typename Result, typename ArgA, typename ArgB>
  inline static constexpr bool kSupportsValue = Supports<Result, ArgA, ArgB>::value;

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result, const ArgA& a, const ArgB& b) {
    if constexpr (std::is_same_v<NullValue, ArgA> || std::is_same_v<NullValue, ArgB>) {
      result = Result{};
    } else {
      result = static_cast<Result>(Functor<std::common_type_t<ArgA, ArgB>>{}(
          static_cast<std::common_type_t<ArgA, ArgB>>(a), static_cast<std::common_type_t<ArgA, ArgB>>(b)));
    }
  }
};

using EqualsEvaluator = StlComparisonFunctorWrapper<std::equal_to>;
using NotEqualsEvaluator = StlComparisonFunctorWrapper<std::not_equal_to>;
using LessThanEvaluator = StlComparisonFunctorWrapper<std::less>;
using LessThanEqualsEvaluator = StlComparisonFunctorWrapper<std::less_equal>;

/**
 * See StlComparisonFunctorWrapper, but for arithmetic functors (e.g., +, -, *).
 */
template <template <typename T> typename Functor>
struct StlArithmeticFunctorWrapper {
  template <typename Result, typename ArgA, typename ArgB>
  struct Supports {
    static constexpr bool kValue = !std::is_same_v<std::string, Result> && !std::is_same_v<std::string, ArgA> &&
                                   !std::is_same_v<std::string, ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result, const ArgA& a, const ArgB& b) {
    if constexpr (std::is_same_v<NullValue, Result> || std::is_same_v<NullValue, ArgA> ||
                  std::is_same_v<NullValue, ArgB>) {
      result = Result{};
    } else {
      result = static_cast<Result>(Functor<std::common_type_t<ArgA, ArgB>>{}(
          static_cast<std::common_type_t<ArgA, ArgB>>(a), static_cast<std::common_type_t<ArgA, ArgB>>(b)));
    }
  }
};

using AdditionEvaluator = StlArithmeticFunctorWrapper<std::plus>;
using SubtractionEvaluator = StlArithmeticFunctorWrapper<std::minus>;
using MultiplicationEvaluator = StlArithmeticFunctorWrapper<std::multiplies>;

/**
 * Modulo selects between the operator % for integrals and std::fmod() for floats. Custom NULL logic returns NULL if
 * the divisor is NULL.
 */
struct ModuloEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct Supports {
    static constexpr bool kValue = !std::is_same_v<std::string, Result> && !std::is_same_v<std::string, ArgA> &&
                                   !std::is_same_v<std::string, ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result_value, bool& result_null, const ArgA& a_value, const bool a_null, const ArgB& b_value,
                  const bool b_null) {
    result_null = a_null || b_null;

    if (result_null) {
      return;
    }

    if constexpr (std::is_same_v<NullValue, Result> || std::is_same_v<NullValue, ArgA> ||
                  std::is_same_v<NullValue, ArgB>) {
      result_value = Result{};
    } else {
      if (b_value == 0) {
        result_null = true;
      } else {
        if constexpr (std::is_integral_v<ArgA> && std::is_integral_v<ArgB>) {
          result_value = static_cast<Result>(a_value % b_value);
        } else {
          result_value = static_cast<Result>(std::fmod(a_value, b_value));
        }
      }
    }
  }
};

/**
 * Custom NULL logic returns NULL if the divisor is NULL.
 */
struct DivisionEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct Supports {
    static constexpr bool kValue = !std::is_same_v<std::string, Result> && !std::is_same_v<std::string, ArgA> &&
                                   !std::is_same_v<std::string, ArgB>;
  };

  template <typename Result, typename ArgA, typename ArgB>
  void operator()(Result& result_value, bool& result_null, const ArgA& a_value, const bool a_null, const ArgB& b_value,
                  const bool b_null) {
    result_null = a_null || b_null;

    if constexpr (std::is_same_v<NullValue, Result> || std::is_same_v<NullValue, ArgA> ||
                  std::is_same_v<NullValue, ArgB>) {
      result_value = Result{};
    } else {
      if (b_value == 0) {
        result_null = true;
      } else {
        result_value = static_cast<Result>(static_cast<Result>(a_value) / static_cast<Result>(b_value));
      }
    }
  }
};

struct CaseEvaluator {
  template <typename Result, typename ArgA, typename ArgB>
  struct Supports {
    static constexpr bool kValue = (std::is_same_v<std::string, ArgA> == std::is_same_v<std::string, ArgB>)&&(
        std::is_same_v<std::string, ArgA> == std::is_same_v<std::string, Result>);
  };

  template <typename Result, typename ArgA, typename ArgB>
  inline static constexpr bool kSupportsV = Supports<Result, ArgA, ArgB>::kValue;
};

}  // namespace skyrise
