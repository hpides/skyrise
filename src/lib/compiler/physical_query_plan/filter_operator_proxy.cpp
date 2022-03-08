#include "filter_operator_proxy.hpp"

#include <sstream>

#include "expression/serialization/expression_deserializer.hpp"
#include "expression/serialization/expression_serializer.hpp"

namespace {

const std::string kJsonKeyPredicate = "predicate";

}  // namespace

namespace skyrise {

FilterOperatorProxy::FilterOperatorProxy(std::shared_ptr<AbstractExpression> predicate)
    : AbstractOperatorProxy(OperatorType::kFilter), predicate_(std::move(predicate)) {}

const std::string& FilterOperatorProxy::Name() const {
  static const std::string kName = "Filter";
  return kName;
}

std::string FilterOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << predicate_->AsColumnName();

  return stream.str();
}

const std::shared_ptr<AbstractExpression>& FilterOperatorProxy::Predicate() const { return predicate_; }

bool FilterOperatorProxy::IsPipelineBreaker() const { return false; }

Aws::Utils::Json::JsonValue FilterOperatorProxy::ToJson() const {
  auto json = AbstractOperatorProxy::ToJson();
  return json.WithObject(kJsonKeyPredicate, ExpressionSerializer::Serialize(*predicate_));
}

std::shared_ptr<AbstractOperatorProxy> FilterOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  auto predicate = ExpressionDeserializer::Deserialize(json.GetObject(kJsonKeyPredicate));
  auto filter_proxy = FilterOperatorProxy::Make(predicate);
  filter_proxy->SetAttributesFromJson(json);

  return filter_proxy;
}

std::shared_ptr<AbstractOperatorProxy> FilterOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return FilterOperatorProxy::Make(predicate_->DeepCopy(), copied_left_input);
}

std::shared_ptr<AbstractOperator> FilterOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail("CreateOperatorInstanceRecursively() is not yet implemented.");
  return nullptr;
}

}  // namespace skyrise
