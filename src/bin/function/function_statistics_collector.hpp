#pragma once

#include "cloud_function/function.hpp"

namespace skyrise {

class FunctionStatisticsCollector : public Function {
 public:
  static constexpr auto kInvalidArguments = "InvalidArguments";
  static constexpr auto kObjectNotAccessible = "ObjectNotAccessible";
  static constexpr auto kIoError = "IoError";
  static constexpr auto kParsingError = "ParsingError";
  static constexpr auto kLogicError = "LogicError";

 protected:
  static bool PayloadIsValid(const Aws::Utils::Json::JsonView& request);
  aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const override;
};

}  // namespace skyrise
