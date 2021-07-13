#include "pqp_serializer.hpp"

#include "pqp_serialization_constants.hpp"

namespace skyrise {

PqpSerializer::PqpSerializer(std::shared_ptr<const AbstractOperatorProxy> root_operator_proxy)
    : root_operator_proxy_(std::move(root_operator_proxy)) {}

std::string PqpSerializer::Serialize() {
  RecursiveSerialize(root_operator_proxy_);

  Aws::Utils::Json::JsonValue result;
  result.WithString(kKeyRootIdentity, root_operator_proxy_->GetIdentity());
  result.WithObject(kKeyOperators, id_to_json_);

  return result.View().WriteReadable();
}

void PqpSerializer::RecursiveSerialize(const std::shared_ptr<const AbstractOperatorProxy>& operator_proxy) {
  std::string id = operator_proxy->GetIdentity();

  if (id_to_json_.View().KeyExists(id)) {
    return;
  }

  id_to_json_.WithObject(id, operator_proxy->ToJson());

  if (operator_proxy->GetLeftInput()) {
    RecursiveSerialize(operator_proxy->GetLeftInput());
  }

  if (operator_proxy->GetRightInput()) {
    RecursiveSerialize(operator_proxy->GetRightInput());
  }
}

}  // namespace skyrise
