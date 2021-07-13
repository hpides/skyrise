#include "import_operator_proxy.hpp"

#include <magic_enum.hpp>

#include "utils/json.hpp"

namespace skyrise {

const std::string& ImportOperatorProxy::Name() const {
  static const auto kName = std::string{"Import"};
  return kName;
}

ImportOperatorProxy::ImportOperatorProxy(std::string bucket_name, std::vector<std::string> objects_keys,
                                         std::vector<ColumnId> pruned_column_ids, ObjectFormat format)
    : AbstractOperatorProxy(OperatorType::kImport),
      bucket_name_(std::move(bucket_name)),
      objects_keys_(std::move(objects_keys)),
      pruned_column_ids_(std::move(pruned_column_ids)),
      format_(format) {}

std::shared_ptr<AbstractOperatorProxy> ImportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json,
                                                                     StorageFactory storage_factory) {
  Aws::String bucket_name = json.GetString("bucket_name");
  ImportOperatorProxy::ObjectFormat format = magic_enum::enum_cast<ObjectFormat>(json.GetString("format")).value();
  std::vector<std::string> object_keys = JsonArrayToVector<std::string>(json.GetArray("object_keys"));
  std::vector<ColumnId> prune_column_ids = JsonArrayToVector<ColumnId>(json.GetArray("pruned_column_ids"));

  auto result = std::make_shared<ImportOperatorProxy>(bucket_name, object_keys, prune_column_ids, format);
  result->SetStorageFactory(std::move(storage_factory));

  return result;
}

Aws::Utils::Json::JsonValue ImportOperatorProxy::ToJson() const {
  return AbstractOperatorProxy::ToJson()
      .WithString("bucket_name", bucket_name_)
      .WithString("format", std::string{magic_enum::enum_name(format_)})
      .WithArray("object_keys", VectorToJsonArray(objects_keys_))
      .WithArray("pruned_column_ids", VectorToJsonArray(pruned_column_ids_));
}

std::shared_ptr<AbstractOperator> ImportOperatorProxy::CreateOperatorInstance() const {
  // TODO(anyone): Replace with actual code which creates and instances of ImportOperator.
  return nullptr;
}

void ImportOperatorProxy::SetStorageFactory(StorageFactory storage_factory) {
  storage_factory_ = std::move(storage_factory);
}

}  // namespace skyrise
