#include "limit_operator_proxy.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "expression/expression_serialization.hpp"

namespace {

const std::string kJsonKeyRowCountExpression = "row_count";
const std::string kName = "Limit";

}  // namespace

namespace skyrise {

LimitOperatorProxy::LimitOperatorProxy(std::shared_ptr<AbstractExpression> row_count)
    : AbstractOperatorProxy(OperatorType::kLimit), row_count_(std::move(row_count)) {}

const std::string& LimitOperatorProxy::Name() const { return kName; }

std::string LimitOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << row_count_->AsColumnName() << " row(s)";
  return stream.str();
}

const std::shared_ptr<AbstractExpression>& LimitOperatorProxy::RowCount() const { return row_count_; }

bool LimitOperatorProxy::IsPipelineBreaker() const { return true; }

Aws::Utils::Json::JsonValue LimitOperatorProxy::ToJson() const {
  auto json = AbstractOperatorProxy::ToJson();
  return json.WithObject(kJsonKeyRowCountExpression, SerializeExpression(*row_count_));
}

std::shared_ptr<AbstractOperatorProxy> LimitOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  auto row_count = DeserializeExpression(json.GetObject(kJsonKeyRowCountExpression));
  auto limit_proxy = LimitOperatorProxy::Make(row_count);
  limit_proxy->SetAttributesFromJson(json);

  return limit_proxy;
}

std::shared_ptr<AbstractOperatorProxy> LimitOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return LimitOperatorProxy::Make(row_count_->DeepCopy(), copied_left_input);
}

size_t LimitOperatorProxy::ShallowHash() const { return row_count_->Hash(); }

std::shared_ptr<AbstractOperator> LimitOperatorProxy::CreateOperatorInstanceRecursively() {
  Fail("CreateOperatorInstanceRecursively() is not yet implemented.");
  return nullptr;
}

}  // namespace skyrise
