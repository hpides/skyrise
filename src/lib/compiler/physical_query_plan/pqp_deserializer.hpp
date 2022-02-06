#pragma once

#include <functional>

#include <aws/core/utils/json/JsonSerializer.h>

#include "abstract_operator_proxy.hpp"

namespace skyrise {

class PqpDeserializer {
 public:
  PqpDeserializer(const std::string& pqp_plan);
  std::shared_ptr<AbstractOperatorProxy> Deserialize();

 private:
  static std::shared_ptr<AbstractOperatorProxy> DeserializeSingleOperator(
      const Aws::Utils::Json::JsonView& operator_parameters);
  void BindInputOperators();

  std::unordered_map<std::string, std::shared_ptr<AbstractOperatorProxy>> operators_;
  Aws::Utils::Json::JsonValue pqp_plan_;
};

}  // namespace skyrise
