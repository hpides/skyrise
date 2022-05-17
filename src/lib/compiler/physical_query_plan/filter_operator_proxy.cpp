#include "filter_operator_proxy.hpp"

#include <sstream>

#include "expression/expression_serialization.hpp"
#include "operator/filter_operator.hpp"

namespace {

const std::string kJsonKeyPredicate = "predicate";
const std::string kName = "Filter";

}  // namespace

namespace skyrise {

FilterOperatorProxy::FilterOperatorProxy(std::shared_ptr<AbstractExpression> predicate)
    : AbstractOperatorProxy(OperatorType::kFilter), predicate_(std::move(predicate)) {}

const std::string& FilterOperatorProxy::Name() const { return kName; }

std::string FilterOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << predicate_->AsColumnName();

  return stream.str();
}

const std::shared_ptr<AbstractExpression>& FilterOperatorProxy::Predicate() const { return predicate_; }

bool FilterOperatorProxy::IsPipelineBreaker() const { return false; }

Aws::Utils::Json::JsonValue FilterOperatorProxy::ToJson() const {
  auto json = AbstractOperatorProxy::ToJson();
  return json.WithObject(kJsonKeyPredicate, SerializeExpression(*predicate_));
}

std::shared_ptr<AbstractOperatorProxy> FilterOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  auto predicate = DeserializeExpression(json.GetObject(kJsonKeyPredicate));
  auto filter_proxy = FilterOperatorProxy::Make(predicate);
  filter_proxy->SetAttributesFromJson(json);

  return filter_proxy;
}

std::shared_ptr<AbstractOperatorProxy> FilterOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return FilterOperatorProxy::Make(predicate_->DeepCopy(), copied_left_input);
}

size_t FilterOperatorProxy::ShallowHash() const { return predicate_->Hash(); }

std::shared_ptr<AbstractOperator> FilterOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(LeftInput(), "Missing input operator proxy.");
  Assert(predicate_, "FilterOperatorProxy has no predicate.");
  return std::make_shared<FilterOperator>(LeftInput()->GetOrCreateOperatorInstance(), predicate_);
}

}  // namespace skyrise
