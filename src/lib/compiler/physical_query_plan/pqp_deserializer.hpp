#pragma once

#include <functional>

#include <aws/core/utils/json/JsonSerializer.h>

#include "abstract_operator_proxy.hpp"

namespace skyrise {

class PqpDeserializer {
 public:
  PqpDeserializer(const std::string& pqp_plan, StorageFactory storage_factory = nullptr);
  std::shared_ptr<const AbstractOperatorProxy> Deserialize();

 private:
  std::shared_ptr<AbstractOperatorProxy> DeserializeSingleOperator(const Aws::Utils::Json::JsonView& operator_payload);
  void BindInputOperators();

  std::unordered_map<std::string, std::shared_ptr<AbstractOperatorProxy>> operators_;
  Aws::Utils::Json::JsonValue pqp_plan_;
  StorageFactory storage_factory_;
};

}  // namespace skyrise
