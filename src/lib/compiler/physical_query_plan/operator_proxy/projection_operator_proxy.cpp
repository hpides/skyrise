#include "projection_operator_proxy.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "expression/expression_serialization.hpp"
#include "expression/expression_utils.hpp"
#include "operator/projection_operator.hpp"

namespace {

const std::string kJsonKeyExpressions = "expressions";
const std::string kName = "Projection";

}  // namespace

namespace skyrise {

ProjectionOperatorProxy::ProjectionOperatorProxy(std::vector<std::shared_ptr<AbstractExpression>> expressions)
    : AbstractOperatorProxy(OperatorType::kProjection), expressions_(std::move(expressions)) {}

const std::string& ProjectionOperatorProxy::Name() const { return kName; }

std::vector<std::shared_ptr<AbstractExpression>> ProjectionOperatorProxy::Expressions() const { return expressions_; }

bool ProjectionOperatorProxy::IsPipelineBreaker() const { return false; }

size_t ProjectionOperatorProxy::OutputColumnsCount() const { return expressions_.size(); }

Aws::Utils::Json::JsonValue ProjectionOperatorProxy::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> expressions_json(expressions_.size());

  for (size_t i = 0; i < expressions_.size(); ++i) {
    expressions_json[i] = SerializeExpression(*expressions_[i]);
  }
  return AbstractOperatorProxy::ToJson().WithArray(kJsonKeyExpressions, expressions_json);
}

std::shared_ptr<AbstractOperatorProxy> ProjectionOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  const auto json_expressions = json.GetArray(kJsonKeyExpressions);
  expressions.reserve(json_expressions.GetLength());

  for (size_t i = 0; i < json_expressions.GetLength(); ++i) {
    auto deserialized_expression = DeserializeExpression(json_expressions.GetItem(i));
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

size_t ProjectionOperatorProxy::ShallowHash() const {
  size_t hash = 0;
  for (const auto& expression : expressions_) {
    boost::hash_combine(hash, expression->Hash());
  }

  return hash;
}

std::shared_ptr<AbstractOperator> ProjectionOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(LeftInput(), "Missing input operator proxy.");
  Assert(!expressions_.empty(), "ProjectionOperatorProxy must specify at least one expression.");
  return std::make_shared<ProjectionOperator>(LeftInput()->GetOrCreateOperatorInstance(), expressions_);
}

}  // namespace skyrise
