#include "pqp_serialization.hpp"

#include "operator_proxy/aggregate_operator_proxy.hpp"
#include "operator_proxy/alias_operator_proxy.hpp"
#include "operator_proxy/export_operator_proxy.hpp"
#include "operator_proxy/filter_operator_proxy.hpp"
#include "operator_proxy/import_operator_proxy.hpp"
#include "operator_proxy/join_operator_proxy.hpp"
#include "operator_proxy/limit_operator_proxy.hpp"
#include "operator_proxy/partition_operator_proxy.hpp"
#include "operator_proxy/projection_operator_proxy.hpp"
#include "operator_proxy/sort_operator_proxy.hpp"
#include "operator_proxy/union_operator_proxy.hpp"

namespace skyrise {

namespace {

inline const std::string kJsonKeyRootOperatorIdentity = "root_operator_identity";
inline const std::string kJsonKeyOperators = "operators";

void SerializeOperatorProxiesRecursively(const std::shared_ptr<const AbstractOperatorProxy>& operator_proxy,
                                         Aws::Utils::Json::JsonValue& operators_json) {
  const std::string operator_identity = operator_proxy->Identity();

  if (operators_json.View().KeyExists(operator_identity)) {
    return;
  }

  // Serialize attributes
  operators_json.WithObject(operator_identity, operator_proxy->ToJson());

  // Recurse to serialize inputs
  if (operator_proxy->LeftInput()) {
    SerializeOperatorProxiesRecursively(operator_proxy->LeftInput(), operators_json);
  }
  if (operator_proxy->RightInput()) {
    SerializeOperatorProxiesRecursively(operator_proxy->RightInput(), operators_json);
  }
}

std::shared_ptr<AbstractOperatorProxy> DeserializeOperatorProxy(const Aws::Utils::Json::JsonView& operator_parameters) {
  const auto maybe_operator_type =
      magic_enum::enum_cast<OperatorType>(operator_parameters.GetString(kJsonKeyOperatorType));
  Assert(maybe_operator_type.has_value(), "Unable to cast operator type.");
  const auto operator_type = maybe_operator_type.value();

  switch (operator_type) {
    case OperatorType::kAggregate:
      return AggregateOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kAlias:
      return AliasOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kExport:
      return ExportOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kFilter:
      return FilterOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kImport:
      return ImportOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kHashJoin:
      return JoinOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kLimit:
      return LimitOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kPartition:
      return PartitionOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kProjection:
      return ProjectionOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kSort:
      return SortOperatorProxy::FromJson(operator_parameters);
    case OperatorType::kUnion:
      return UnionOperatorProxy::FromJson(operator_parameters);
    default:
      Fail("Unknown operator type.");
  }
}

}  // namespace

std::string SerializePqp(const std::shared_ptr<const AbstractOperatorProxy>& root_operator_proxy) {
  Aws::Utils::Json::JsonValue pqp_json;

  // (1) Serialize PQP metadata
  pqp_json.WithString(kJsonKeyRootOperatorIdentity, root_operator_proxy->Identity());

  // (2) Serialize PQP's operator proxies
  Aws::Utils::Json::JsonValue operator_proxies_json;
  SerializeOperatorProxiesRecursively(root_operator_proxy, operator_proxies_json);
  pqp_json.WithObject(kJsonKeyOperators, operator_proxies_json);

  return pqp_json.View().WriteReadable();
}

std::shared_ptr<AbstractOperatorProxy> DeserializePqp(const std::string& pqp_string) {
  const Aws::Utils::Json::JsonValue pqp_json(pqp_string);
  Assert(pqp_json.View().KeyExists(kJsonKeyRootOperatorIdentity),
         "Attribute '" + kJsonKeyRootOperatorIdentity + "' is required.");
  Assert(pqp_json.View().KeyExists(kJsonKeyOperators), "Attribute '" + kJsonKeyOperators + "' is required.");
  const auto json_view = pqp_json.View();

  // (1) Deserialize into a flat list of operator proxies
  const auto operators_json_map = json_view.GetObject(kJsonKeyOperators).GetAllObjects();
  std::unordered_map<std::string, std::shared_ptr<AbstractOperatorProxy>> identity_to_operator_proxies;
  identity_to_operator_proxies.reserve(operators_json_map.size());

  for (const auto& [identity, operator_parameters] : operators_json_map) {
    identity_to_operator_proxies.emplace(identity, DeserializeOperatorProxy(operator_parameters));
  }

  // (2) Bind operator proxies' inputs
  for (auto& [identity, operator_proxy] : identity_to_operator_proxies) {
    operator_proxy->BindInputs(identity_to_operator_proxies);
  }

  auto root_operator_identity = json_view.GetString(kJsonKeyRootOperatorIdentity);
  return identity_to_operator_proxies[root_operator_identity];
}

}  // namespace skyrise
