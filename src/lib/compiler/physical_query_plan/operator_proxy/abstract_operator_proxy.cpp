#include "abstract_operator_proxy.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>
#include <magic_enum/magic_enum.hpp>

#include "compiler/physical_query_plan/pqp_utils.hpp"
#include "operator/abstract_operator.hpp"
#include "utils/assert.hpp"
#include "utils/print_directed_acyclic_graph.hpp"

namespace {

constexpr std::string_view kJsonKeyComment = "comment";
constexpr std::string_view kJsonKeyLeftInputOperatorIdentity = "left_input_operator_identity";
constexpr std::string_view kJsonKeyOperatorIdentity = "operator_identity";
constexpr std::string_view kJsonKeyRightInputOperatorIdentity = "right_input_operator_identity";

}  // namespace

namespace skyrise {

AbstractOperatorProxy::AbstractOperatorProxy(const OperatorType type) : type_(type) {}

OperatorType AbstractOperatorProxy::Type() const { return type_; }

std::string AbstractOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "[" << Name() << "]";

  if (!comment_.empty()) {
    const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
    stream << separator << "(" << comment_ << ")";
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
  Assert(!prefix.empty(), "Prefix must not be empty.");
  std::stringstream stream;
  stream << prefix << "-" << Identity();
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
  const auto copied_proxies_iterator = copied_proxies.find(this);
  if (copied_proxies_iterator != copied_proxies.end()) {
    return copied_proxies_iterator->second;
  }

  const auto copied_left_input =
      LeftInput() ? LeftInput()->DeepCopy(copied_proxies) : std::shared_ptr<AbstractOperatorProxy>();
  const auto copied_right_input =
      RightInput() ? RightInput()->DeepCopy(copied_proxies) : std::shared_ptr<AbstractOperatorProxy>();

  auto copied_proxy = OnDeepCopy(copied_left_input, copied_right_input);
  copied_proxy->SetIdentity(Identity());
  copied_proxy->SetComment(comment_);

  copied_proxies.emplace(this, copied_proxy);

  return copied_proxy;
}

size_t AbstractOperatorProxy::Hash() const {
  size_t hash = 0;

  VisitPqp(SharedFromBase(), [&hash](const auto& node) {
    if (node) {
      boost::hash_combine(hash, node->type_);
      boost::hash_combine(hash, node->ShallowHash());
      return PqpVisitation::kVisitInputs;
    } else {
      return PqpVisitation::kDoNotVisitInputs;
    }
  });

  return hash;
}

std::shared_ptr<AbstractOperator> AbstractOperatorProxy::GetOrCreateOperatorInstance() {
  if (!operator_instance_) {
    operator_instance_ = CreateOperatorInstanceRecursively();
  }

  return operator_instance_;
}

Aws::Utils::Json::JsonValue AbstractOperatorProxy::ToJson() const {
  Aws::Utils::Json::JsonValue result;
  result.WithString(std::string{kJsonKeyOperatorType}, std::string(magic_enum::enum_name(type_)))
      .WithString(std::string{kJsonKeyOperatorIdentity}, Identity());

  // Serialize inputs with operator identity strings.
  if (LeftInput()) {
    result.WithString(std::string{kJsonKeyLeftInputOperatorIdentity}, LeftInput()->Identity());
  }

  if (RightInput()) {
    result.WithString(std::string{kJsonKeyRightInputOperatorIdentity}, RightInput()->Identity());
  }

  if (!comment_.empty()) {
    result.WithString(std::string{kJsonKeyComment}, comment_);
  }

  return result;
}

void AbstractOperatorProxy::BindInputs(
    const std::unordered_map<std::string, std::shared_ptr<AbstractOperatorProxy>>& identity_to_operator_proxies) {
  Assert(!LeftInput() && !RightInput(), "Inputs are expected to be unset.");

  // Bind left input, if specified.
  if (left_input_identity_.empty()) {
    Assert(right_input_identity_.empty(), "Unexpected right input operator identity.");
    return;
  }
  const auto& left_input = identity_to_operator_proxies.find(left_input_identity_);
  Assert(left_input != identity_to_operator_proxies.end(),
         "Left input operator proxy cannot be bound because no instance was provided.");
  SetLeftInput(left_input->second);
  left_input_identity_.clear();

  // Bind right input, if specified.
  if (right_input_identity_.empty()) {
    return;
  }
  const auto& right_input = identity_to_operator_proxies.find(right_input_identity_);
  Assert(right_input != identity_to_operator_proxies.end(),
         "Right input operator proxy cannot be bound because no instance was provided.");
  SetRightInput(right_input->second);
  right_input_identity_.clear();
}

void AbstractOperatorProxy::SetAttributesFromJson(const Aws::Utils::Json::JsonView& json) {
  Assert(json.KeyExists(std::string{kJsonKeyOperatorIdentity}), "Expected operator proxy identity in JSON.");
  identity_ = json.GetString(std::string{kJsonKeyOperatorIdentity});

  if (json.KeyExists(std::string{kJsonKeyLeftInputOperatorIdentity})) {
    left_input_identity_ = json.GetString(std::string{kJsonKeyLeftInputOperatorIdentity});
  }

  if (json.KeyExists(std::string{kJsonKeyRightInputOperatorIdentity})) {
    right_input_identity_ = json.GetString(std::string{kJsonKeyRightInputOperatorIdentity});
  }

  if (json.KeyExists(std::string{kJsonKeyComment})) {
    comment_ = json.GetString(std::string{kJsonKeyComment});
  }
}

std::ostream& operator<<(std::ostream& stream, const AbstractOperatorProxy& root_operator_proxy) {
  // Functor returns the inputs of a given node.
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

  // Functor writes a given node's description to a given output stream.
  const auto print_node = [](const auto& operator_proxy, auto& output_stream) {
    output_stream << operator_proxy->Description(DescriptionMode::kSingleLine);
  };

  PrintDirectedAcyclicGraph<const AbstractOperatorProxy>(root_operator_proxy.SharedFromBase(), get_inputs, print_node,
                                                         stream);

  return stream;
}

}  // namespace skyrise
