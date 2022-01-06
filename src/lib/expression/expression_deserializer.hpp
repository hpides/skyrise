#pragma once

#include <aws/core/utils/json/JsonSerializer.h>

#include "expression/abstract_expression.hpp"

namespace skyrise {

class ExpressionDeserializer {
 public:
  static std::shared_ptr<AbstractExpression> Deserialize(Aws::Utils::Json::JsonView json);

 private:
  static std::vector<std::shared_ptr<AbstractExpression>> DeserializeArguments(Aws::Utils::Json::JsonView json);
};

}  // namespace skyrise
