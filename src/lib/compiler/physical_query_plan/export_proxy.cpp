#include "export_operator_proxy.hpp"
#include "utils/json.hpp"

namespace skyrise {

const std::string& ExportOperatorProxy::Name() const {
  static const auto kName = std::string{"Export"};
  return kName;
}

ExportOperatorProxy::ExportOperatorProxy(std::string bucket_name, std::string target_object_key,
                                         const std::shared_ptr<const AbstractOperatorProxy>& left,
                                         const std::shared_ptr<const AbstractOperatorProxy>& right)
    : AbstractOperatorProxy(OperatorType::kExport, left, right),
      bucket_name_(std::move(bucket_name)),
      target_object_key_(std::move(target_object_key)) {}

std::shared_ptr<AbstractOperatorProxy> ExportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json,
                                                                     StorageFactory storage_factory) {
  auto bucket_name = json.GetString("bucket_name");
  auto target_object_key = json.GetString("target_object_key");

  // We do not execute the binding here. Thus, left_ and right_ are nullptr at this point and later be set.

  auto result = std::make_shared<ExportOperatorProxy>(bucket_name, target_object_key);
  result->SetStorageFactory(std::move(storage_factory));

  return result;
}

Aws::Utils::Json::JsonValue ExportOperatorProxy::ToJson() const {
  return AbstractOperatorProxy::ToJson()
      .WithString("bucket_name", bucket_name_)
      .WithString("target_object_key", target_object_key_);
}

std::shared_ptr<AbstractOperator> ExportOperatorProxy::CreateOperatorInstance() const {
  // TODO(anyone): Replace with actual code which creates instances of ExportOperator.
  return nullptr;
}

void ExportOperatorProxy::SetStorageFactory(StorageFactory storage_factory) {
  storage_factory_ = std::move(storage_factory);
}

}  // namespace skyrise
