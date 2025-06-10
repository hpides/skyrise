#pragma once

#include <aws/core/utils/json/JsonSerializer.h>

#include "expression/abstract_expression.hpp"

namespace skyrise {

Aws::Utils::Json::JsonValue SerializeExpression(const AbstractExpression& expression);
Aws::Utils::Json::JsonValue SerializeExpression(const std::shared_ptr<AbstractExpression>& expression);

std::shared_ptr<AbstractExpression> DeserializeExpression(Aws::Utils::Json::JsonView json);

}  // namespace skyrise
