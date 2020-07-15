/**
 * Taken and modified from our Hyrise sister project (https://github.com/hyrise/hyrise at commit b856b57)
 *
 * Changelog:
 * - Change namespace
 */

#pragma once

#include <exception>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace skyrise {

/*
 * Hyrise specific exception used to handle errors related to wrong user input.
 * The console will catch this exception when parsing a sql string.
 * Also thrown by the macro AssertInput(expr, msg) to easily check user input related constraints.
 */
class InvalidInputException : public std::runtime_error {
 public:
  explicit InvalidInputException(const std::string& what_arg) : std::runtime_error(what_arg) {}
};

}  // namespace skyrise
