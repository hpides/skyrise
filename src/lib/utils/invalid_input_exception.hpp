/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <stdexcept>
#include <string>

namespace skyrise {

// Handle errors related to wrong user input
// (e.g., through the AssertInput macro)
class InvalidInputException : public std::runtime_error {
 public:
  explicit InvalidInputException(const std::string& what) : std::runtime_error(what) {}
};

}  // namespace skyrise
