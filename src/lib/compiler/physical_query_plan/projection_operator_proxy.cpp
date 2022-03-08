#include "projection_operator_proxy.hpp"

#include <sstream>

#include "expression/expression_utils.hpp"
#include "expression/serialization/expression_deserializer.hpp"
#include "expression/serialization/expression_serializer.hpp"

namespace {

const std::string kJsonKeyExpressions = "expressions";

}  // namespace

namespace skyrise {

ProjectionOperatorProxy::ProjectionOperatorProxy(std::vector<std::shared_ptr<AbstractExpression>> expressions)
    : AbstractOperatorProxy(OperatorType::kProjection), expressions_(std::move(expressions)) {}

const std::string& ProjectionOperatorProxy::Name() const {
  static const std::string kName = "Projection";
  return kName;
}

const std::vector<std::shared_ptr<AbstractExpression>> ProjectionOperatorProxy::Expressions() const {
  return expressions_;
}

bool ProjectionOperatorProxy::IsPipelineBreaker() const { return false; }

size_t ProjectionOperatorProxy::OutputColumnsCount() const { return expressions_.size(); }

Aws::Utils::Json::JsonValue ProjectionOperatorProxy::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> expressions_json(expressions_.size());
  for (size_t i = 0; i < expressions_.size(); i++) {
    expressions_json[i] = ExpressionSerializer::Serialize(*expressions_[i]);
  }
  return AbstractOperatorProxy::ToJson().WithArray(kJsonKeyExpressions, expressions_json);
}

std::shared_ptr<AbstractOperatorProxy> ProjectionOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  auto expressions_json_array = json.GetArray(kJsonKeyExpressions);
  expressions.reserve(expressions_json_array.GetLength());
  for (size_t i = 0; i < expressions_json_array.GetLength(); ++i) {
    auto deserialized_expression = ExpressionDeserializer::Deserialize(expressions_json_array.GetItem(i));
    expressions.emplace_back(deserialized_expression);
  }

  auto projection_proxy = ProjectionOperatorProxy::Make(expressions);
  projection_proxy->SetAttributesFromJson(json);

  return projection_proxy;
}

std::shared_ptr<AbstractOperatorProxy> ProjectionOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return ProjectionOperatorProxy::Make(ExpressionsDeepCopy(expressions_), copied_left_input);
}

std::shared_ptr<AbstractOperator> ProjectionOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail("CreateOperatorInstanceRecursively() is not yet implemented.");
  return nullptr;
}

}  // namespace skyrise
