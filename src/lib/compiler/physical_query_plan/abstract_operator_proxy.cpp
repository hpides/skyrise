#include "abstract_operator_proxy.hpp"

#include <sstream>

#include <magic_enum.hpp>

#include "operator/abstract_operator.hpp"
#include "utils/assert.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

namespace {
inline const std::string kJsonKeyComment = "comment";
inline const std::string kJsonKeyLeftInputOperatorIdentity = "left_input_operator_identity";
inline const std::string kJsonKeyOperatorIdentity = "operator_identity";
inline const std::string kJsonKeyRightInputOperatorIdentity = "right_input_operator_identity";

}  // namespace

namespace skyrise {

AbstractOperatorProxy::AbstractOperatorProxy(const OperatorType type) : type_(type) {}

OperatorType AbstractOperatorProxy::Type() const { return type_; }

std::string AbstractOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "[" << Name() << "]";

  // Append comment, if set
  if (!comment_.empty()) {
    const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
    stream << separator;
    stream << "(" << comment_ << ")";
  }

  return stream.str();
}

std::string AbstractOperatorProxy::Identity() const {
  if (identity_.empty()) {
    // Generate a default identity for this operator proxy.
    // To guarantee uniqueness, incorporate the instance address, which is unique by definition.
    // Since we call the virtual function Name(), we cannot easily put this code into the constructor.
    std::stringstream stream;
    stream << Name() << this;
    identity_ = stream.str();
  }
  return identity_;
}

void AbstractOperatorProxy::PrefixIdentity(const std::string& prefix) {
  Assert(!prefix.empty(), "Unexpected empty prefix!");
  std::stringstream stream;
  stream << prefix << Identity();
  identity_ = stream.str();
}

void AbstractOperatorProxy::SetIdentity(const std::string& identity) {
  Assert(!identity.empty(), "Expected non-empty identity string.");
  identity_ = identity;
}

size_t AbstractOperatorProxy::InputObjectsCount() const {
  size_t input_objects_count = 0;
  if (LeftInput()) {
    input_objects_count += LeftInput()->OutputObjectsCount();
  }
  if (RightInput()) {
    input_objects_count += RightInput()->OutputObjectsCount();
  }
  return input_objects_count;
}

size_t AbstractOperatorProxy::OutputObjectsCount() const {
  DebugAssert(!RightInput(), "Did not expect right input.");
  return LeftInput()->OutputObjectsCount();
}

size_t AbstractOperatorProxy::OutputColumnsCount() const {
  DebugAssert(!RightInput(), "Did not expect right input.");
  return LeftInput()->OutputColumnsCount();
}

std::shared_ptr<AbstractOperatorProxy> AbstractOperatorProxy::DeepCopy() const {
  std::unordered_map<const AbstractOperatorProxy*, std::shared_ptr<AbstractOperatorProxy>> copied_proxies;
  return DeepCopy(copied_proxies);
}

std::shared_ptr<AbstractOperatorProxy> AbstractOperatorProxy::DeepCopy(
    std::unordered_map<const AbstractOperatorProxy*, std::shared_ptr<AbstractOperatorProxy>>& copied_proxies) const {
  const auto copied_proxies_iter = copied_proxies.find(this);
  if (copied_proxies_iter != copied_proxies.end()) {
    return copied_proxies_iter->second;
  }

  const auto copied_left_input =
      LeftInput() ? LeftInput()->DeepCopy(copied_proxies) : std::shared_ptr<AbstractOperatorProxy>();
  const auto copied_right_input =
      RightInput() ? RightInput()->DeepCopy(copied_proxies) : std::shared_ptr<AbstractOperatorProxy>();

  auto copied_op = OnDeepCopy(copied_left_input, copied_right_input);
  copied_op->SetIdentity(Identity());
  copied_op->SetComment(comment_);

  copied_proxies.emplace(this, copied_op);

  return copied_op;
}

std::shared_ptr<AbstractOperator> AbstractOperatorProxy::GetOrCreateOperatorInstance() {
  if (!operator_instance_) {
    operator_instance_ = CreateOperatorInstanceRecursively();
  }

  return operator_instance_;
}

Aws::Utils::Json::JsonValue AbstractOperatorProxy::ToJson() const {
  Aws::Utils::Json::JsonValue result;
  result.WithString(kJsonKeyOperatorType, std::string(magic_enum::enum_name(type_)))
      .WithString(kJsonKeyOperatorIdentity, Identity());

  // Serialize inputs with operator identity strings
  if (LeftInput()) {
    result.WithString(kJsonKeyLeftInputOperatorIdentity, LeftInput()->Identity());
  }
  if (RightInput()) {
    result.WithString(kJsonKeyRightInputOperatorIdentity, RightInput()->Identity());
  }

  if (!comment_.empty()) {
    result.WithString(kJsonKeyComment, comment_);
  }

  return result;
}

void AbstractOperatorProxy::BindInputs(
    const std::unordered_map<std::string, std::shared_ptr<AbstractOperatorProxy>>& operator_proxies_by_identity) {
  Assert(!LeftInput() && !RightInput(), "Inputs are expected to be unset.");

  // Bind left input, if specified.
  if (left_input_identity_.empty()) {
    Assert(right_input_identity_.empty(), "Unexpected right input operator identity.");
    return;
  }
  // TODO(julianmenzler): C++20: Replace with .contains
  Assert(operator_proxies_by_identity.find(left_input_identity_) != operator_proxies_by_identity.end(),
         "Left input operator proxy cannot be bound because no instance was provided.");
  SetLeftInput(operator_proxies_by_identity.at(left_input_identity_));
  left_input_identity_.clear();

  // Bind right input, if specified.
  if (right_input_identity_.empty()) {
    return;
  }
  // TODO(julianmenzler): C++20: Replace with .contains
  Assert(operator_proxies_by_identity.find(right_input_identity_) != operator_proxies_by_identity.end(),
         "Right input operator proxy cannot be bound because no instance was provided.");
  SetRightInput(operator_proxies_by_identity.at(right_input_identity_));
  right_input_identity_.clear();
}

void AbstractOperatorProxy::SetAttributesFromJson(const Aws::Utils::Json::JsonView& json) {
  Assert(json.KeyExists(kJsonKeyOperatorIdentity), "Expected operator proxy identity in JSON.");
  identity_ = json.GetString(kJsonKeyOperatorIdentity);

  if (json.KeyExists(kJsonKeyLeftInputOperatorIdentity)) {
    left_input_identity_ = json.GetString(kJsonKeyLeftInputOperatorIdentity);
  }

  if (json.KeyExists(kJsonKeyRightInputOperatorIdentity)) {
    right_input_identity_ = json.GetString(kJsonKeyRightInputOperatorIdentity);
  }

  if (json.KeyExists(kJsonKeyComment)) {
    comment_ = json.GetString(kJsonKeyComment);
  }
}

std::ostream& operator<<(std::ostream& stream, const AbstractOperatorProxy& root_operator_proxy) {
  // Functor returning the inputs of a given node
  const auto get_inputs = [](const auto& operator_proxy) {
    std::vector<std::shared_ptr<const AbstractOperatorProxy>> inputs;
    if (operator_proxy->LeftInput()) {
      inputs.emplace_back(operator_proxy->LeftInput());
    }
    if (operator_proxy->RightInput()) {
      inputs.emplace_back(operator_proxy->RightInput());
    }
    return inputs;
  };

  // Functor writing a given node's description to a given output stream
  const auto print_node = [](const auto& operator_proxy, auto& output_stream) {
    output_stream << operator_proxy->Description(DescriptionMode::kSingleLine);
    //    output_stream << " @ " << operator_proxy;
    //    output_stream << " @ " << operator_proxy->Identity();
  };

  PrintDirectedAcyclicGraph<const AbstractOperatorProxy>(root_operator_proxy.SharedFromBase(), get_inputs, print_node,
                                                         stream);

  return stream;
}

}  // namespace skyrise
