#include "export_operator_proxy.hpp"

#include "utils/json.hpp"

namespace skyrise {

const std::string& ExportOperatorProxy::Name() const {
  static const std::string kName("Export");
  return kName;
}

ExportOperatorProxy::ExportOperatorProxy(std::string bucket_name, std::string target_object_key,
                                         ExportOperator::OutputFormat output_format,
                                         const std::shared_ptr<const AbstractOperatorProxy>& left,
                                         const std::shared_ptr<const AbstractOperatorProxy>& right)
    : AbstractOperatorProxy(OperatorType::kExport, left, right),
      bucket_name_(std::move(bucket_name)),
      target_object_key_(std::move(target_object_key)),
      output_format_(output_format) {}

std::shared_ptr<AbstractOperatorProxy> ExportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json,
                                                                     StorageFactory storage_factory) {
  auto bucket_name = json.GetString("bucket_name");
  auto target_object_key = json.GetString("target_object_key");
  auto output_format = *magic_enum::enum_cast<ExportOperator::OutputFormat>(json.GetString("output_format"));

  auto result = std::make_shared<ExportOperatorProxy>(bucket_name, target_object_key, output_format);
  result->SetStorageFactory(std::move(storage_factory));
  return result;
}

Aws::Utils::Json::JsonValue ExportOperatorProxy::ToJson() const {
  return AbstractOperatorProxy::ToJson()
      .WithString("bucket_name", bucket_name_)
      .WithString("target_object_key", target_object_key_)
      .WithString("output_format", std::string(magic_enum::enum_name(output_format_)));
}

std::shared_ptr<AbstractOperator> ExportOperatorProxy::CreateOperatorInstance() const {
  return std::make_shared<ExportOperator>(GetLeftInput() ? GetLeftInput()->GetOperatorInstance() : nullptr,
                                          storage_factory_(bucket_name_), target_object_key_, output_format_);
}

void ExportOperatorProxy::SetStorageFactory(StorageFactory storage_factory) {
  storage_factory_ = std::move(storage_factory);
}

}  // namespace skyrise
