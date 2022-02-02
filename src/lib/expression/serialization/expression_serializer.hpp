#pragma once

#include <aws/core/utils/json/JsonSerializer.h>

#include "expression/abstract_expression.hpp"

namespace skyrise {

class ExpressionSerializer {
 public:
  static Aws::Utils::Json::JsonValue Serialize(const AbstractExpression& expression);
  static Aws::Utils::Json::JsonValue Serialize(const std::shared_ptr<AbstractExpression>& expression);

 private:
  static Aws::Utils::Array<Aws::Utils::Json::JsonValue> SerializeArguments(const AbstractExpression& expression);
};

}  // namespace skyrise
