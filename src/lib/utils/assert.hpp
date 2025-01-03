/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#pragma once

#include <exception>
#include <string>

#include "invalid_input_exception.hpp"
#include "string.hpp"

/**
 * This file provides better assertions than the std cassert/assert.h
 *
 * DebugAssert(condition, msg) and Fail(msg) can be used to both harden code by programming by contract and document the
 * invariants enforced in messages.
 *
 * --> Use DebugAssert() whenever a certain invariant must hold, as in
 *
 * int divide(int numerator, int denominator) {
 *   DebugAssert(denominator == 0, "Divisions by zero are not allowed.");
 *   return numerator / denominator;
 * }
 *
 * --> Use Fail() whenever an illegal code path is taken. Especially useful for switch statements:
 *
 * void foo(int v) {
 *   switch(v) {
 *     case 0: //...
 *     case 3: //...
 *     case 17: //...
 *     default: Fail("Illegal parameter");
 * }
 *
 * --> Use Assert() whenever an invariant should be checked even in release builds, either because testing it is
 *     very cheap or the invariant is considered very important
 *
 * --> Use AssertInput() to check if the user input is correct. This provides a more specific error handling since an
 *     invalid input might want to be caught.
 */

namespace skyrise {

namespace detail {

// We need this indirection so that we can throw exceptions from destructors without the compiler complaining. That is
// generally forbidden and might lead to std::terminate, but since we don't want to handle most errors anyway,
// that's fine.
[[noreturn]] inline void Fail(const std::string& message) { throw std::logic_error(message); }

}  // namespace detail

#define Fail(message)                                                                                              \
  skyrise::detail::Fail(skyrise::TrimSourceFilePath(__FILE__) + ":" + std::to_string(__LINE__) + " " + (message)); \
  static_assert(true, "End macro call with a semicolon")

[[noreturn]] inline void FailInput(const std::string& message) {
  throw InvalidInputException(std::string("Error: Invalid input; ") + message);
}

// StaticFail allows compile-time errors in combination with if constexpr.
// See https://stackoverflow.com/questions/38304847/constexpr-if-and-static-assert.
template <bool Flag = false>
void StaticFail() {
  static_assert(Flag, "Static assertion failed.");
}

}  // namespace skyrise

#define Assert(expression, message)     \
  if (!static_cast<bool>(expression)) { \
    Fail(message);                      \
  }                                     \
  static_assert(true, "End macro call with a semicolon")

#define AssertInput(expression, message)                                            \
  if (!static_cast<bool>(expression)) {                                             \
    throw InvalidInputException(std::string("Error: Invalid input; ") + (message)); \
  }                                                                                 \
  static_assert(true, "End macro call with a semicolon")

#if SKYRISE_DEBUG
#define DebugAssert(expression, message) Assert(expression, message)
#else
#define DebugAssert(expression, message)
#endif
