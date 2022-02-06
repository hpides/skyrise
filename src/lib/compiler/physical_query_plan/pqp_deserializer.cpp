#include "pqp_deserializer.hpp"

#include <unordered_map>

#include <magic_enum.hpp>
#include <utils/assert.hpp>

#include "export_operator_proxy.hpp"
#include "import_operator_proxy.hpp"
#include "partition_operator_proxy.hpp"
#include "pqp_serialization_constants.hpp"

namespace skyrise {

PqpDeserializer::PqpDeserializer(const std::string& pqp_plan) : pqp_plan_(pqp_plan) {
  Assert(pqp_plan_.View().KeyExists(kKeyRootIdentity), "Attribute 'root_identity' is required.");
  Assert(pqp_plan_.View().KeyExists(kKeyOperators), "Attribute 'operators' is required.");
}

std::shared_ptr<AbstractOperatorProxy> PqpDeserializer::DeserializeSingleOperator(
    const Aws::Utils::Json::JsonView& operator_parameters) {
  const auto maybe_operator_type = magic_enum::enum_cast<OperatorType>(operator_parameters.GetString(kKeyOperatorType));

  Assert(maybe_operator_type.has_value(), "Unable to cast operator type.");

  switch (maybe_operator_type.value()) {
    case OperatorType::kImport:
      return ImportOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kExport:
      return ExportOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kPartition:
      return PartitionOperatorProxy::FromJson(operator_parameters);
    default:
      Fail("Unknown operator type.");
  }
}

void PqpDeserializer::BindInputOperators() {
  for (const auto& identity_operator_pair : pqp_plan_.View().GetObject(kKeyOperators).GetAllObjects()) {
    const Aws::String& identity = identity_operator_pair.first;
    const Aws::Utils::Json::JsonView& operator_parameters = identity_operator_pair.second;
    std::shared_ptr<skyrise::AbstractOperatorProxy>& unbound_operator = operators_[identity];

    if (operator_parameters.KeyExists(kKeyRightInput)) {
      const auto right_input_identity = operator_parameters.GetString(kKeyRightInput);
      Assert(operators_.find(right_input_identity) != operators_.end(),
             "Unable to find right operator identity in the operator map.");
      unbound_operator->SetRightInput(operators_[right_input_identity]);
    }
    if (operator_parameters.KeyExists(kKeyLeftInput)) {
      const auto left_input_identity = operator_parameters.GetString(kKeyLeftInput);
      Assert(operators_.find(left_input_identity) != operators_.end(),
             "Unable to find left operator identity in the operator map.");
      unbound_operator->SetLeftInput(operators_[left_input_identity]);
    }
  }
}

std::shared_ptr<AbstractOperatorProxy> PqpDeserializer::Deserialize() {
  const auto view = pqp_plan_.View();

  const auto operator_map = view.GetObject(kKeyOperators).GetAllObjects();
  operators_.reserve(operator_map.size());

  for (const auto& [identity, operator_parameters] : operator_map) {
    operators_.emplace(identity, DeserializeSingleOperator(operator_parameters));
  }

  BindInputOperators();

  auto root_operator = operators_[view.GetString(kKeyRootIdentity)];
  return root_operator;
}

}  // namespace skyrise
