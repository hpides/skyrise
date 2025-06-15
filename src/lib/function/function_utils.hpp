#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace skyrise {

/**
 * Utility functions for working with functions and callbacks.
 */
namespace FunctionUtils {

/**
 * Creates a function that will be executed when the returned object is destroyed.
 * @param func The function to execute
 * @return A unique_ptr that will execute the function when destroyed
 */
template <typename Func>
std::unique_ptr<void, std::function<void(void*)>> MakeScopedFunction(Func&& func) {
  return std::unique_ptr<void, std::function<void(void*)>>(nullptr, [func = std::forward<Func>(func)](void*) { func(); });
}

/**
 * Creates a function that will be executed when the returned object is destroyed.
 * @param func The function to execute
 * @return A shared_ptr that will execute the function when destroyed
 */
template <typename Func>
std::shared_ptr<void> MakeSharedScopedFunction(Func&& func) {
  return std::shared_ptr<void>(nullptr, [func = std::forward<Func>(func)](void*) { func(); });
}

}  // namespace FunctionUtils

}  // namespace skyrise 