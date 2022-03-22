#pragma once

#include "function/function.hpp"

namespace skyrise {

class FunctionStatisticsCollector : public Function {
 public:
  inline static const std::string kInvalidArguments = "InvalidArguments";
  inline static const std::string kObjectNotAccessible = "ObjectNotAccessible";
  inline static const std::string kIoError = "IoError";
  inline static const std::string kParsingError = "ParsingError";
  inline static const std::string kLogicError = "LogicError";

 protected:
  static bool PayloadIsValid(const Aws::Utils::Json::JsonView& request);
  aws::lambda_runtime::invocation_response OnHandleRequest(const Aws::Utils::Json::JsonView& request) const override;
};

}  // namespace skyrise
