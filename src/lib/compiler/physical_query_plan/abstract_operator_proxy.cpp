#include "abstract_operator_proxy.hpp"

#include <sstream>

#include <magic_enum.hpp>

#include "pqp_serialization_constants.hpp"
#include "utils/assert.hpp"

namespace skyrise {

AbstractOperatorProxy::AbstractOperatorProxy(const OperatorType type, std::shared_ptr<AbstractOperatorProxy> left,
                                             std::shared_ptr<AbstractOperatorProxy> right)
    : type_(type), left_input_(std::move(left)), right_input_(std::move(right)) {}

OperatorType AbstractOperatorProxy::Type() const { return type_; }

std::string AbstractOperatorProxy::Description(DescriptionMode /* description_mode */) const { return Name(); }

Aws::Utils::Json::JsonValue AbstractOperatorProxy::ToJson() const {
  Aws::Utils::Json::JsonValue result;
  result.WithString(kKeyOperatorType, std::string{magic_enum::enum_name(type_)});
  if (left_input_) {
    result.WithString(kKeyLeftInput, left_input_->GetIdentity());
  }
  if (right_input_) {
    result.WithString(kKeyRightInput, right_input_->GetIdentity());
  }
  return result;
}

std::string AbstractOperatorProxy::GetIdentity() const {
  const auto* address = static_cast<const void*>(this);
  std::stringstream string_stream;
  string_stream << address;

  return string_stream.str();
}

std::shared_ptr<AbstractOperatorProxy> AbstractOperatorProxy::GetLeftInput() const { return left_input_; }

std::shared_ptr<AbstractOperatorProxy> AbstractOperatorProxy::GetRightInput() const { return right_input_; }

void AbstractOperatorProxy::SetLeftInput(std::shared_ptr<AbstractOperatorProxy> left_input) {
  left_input_ = std::move(left_input);
}
void AbstractOperatorProxy::SetRightInput(std::shared_ptr<AbstractOperatorProxy> right_input) {
  right_input_ = std::move(right_input);
}

std::shared_ptr<AbstractOperator> AbstractOperatorProxy::GetOrCreateOperatorInstance() {
  if (!operator_instance_) {
    operator_instance_ = CreateOperatorInstance();
  }

  return operator_instance_;
}

}  // namespace skyrise
