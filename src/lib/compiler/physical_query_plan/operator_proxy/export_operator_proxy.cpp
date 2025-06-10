#include "export_operator_proxy.hpp"

#include <boost/container_hash/hash.hpp>
#include <magic_enum/magic_enum.hpp>

#include "operator/export_operator.hpp"
#include "utils/json.hpp"

namespace {

const std::string kJsonKeyBucketName = "bucket_name";
const std::string kJsonKeyExportFormat = "export_format";
const std::string kJsonKeyTargetObjectKey = "target_object_key";
const std::string kName = "Export";
const std::string kPlaceholderString = "Placeholder";

}  // namespace

namespace skyrise {

ExportOperatorProxy::ExportOperatorProxy(ObjectReference target_object, FileFormat export_format)
    : AbstractOperatorProxy(OperatorType::kExport),
      target_object_(std::move(target_object)),
      export_format_(export_format) {
  Assert(target_object_.etag.empty(), "Target ObjectReference should not specify an ETag attribute.");
}

const std::string& ExportOperatorProxy::Name() const { return kName; }

std::string ExportOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << target_object_.bucket_name << "/";
  if (mode == DescriptionMode::kMultiLine) {
    stream << separator;
  }
  stream << target_object_.identifier;
  return stream.str();
}

void ExportOperatorProxy::SetTargetObject(ObjectReference target_object, FileFormat export_format) {
  Assert(target_object.etag.empty(), "Target ObjectReference should not specify an ETag attribute.");
  target_object_ = std::move(target_object);
  export_format_ = export_format;
}

const ObjectReference& ExportOperatorProxy::TargetObject() const { return target_object_; }

FileFormat ExportOperatorProxy::GetExportFormat() const { return export_format_; }

bool ExportOperatorProxy::IsPipelineBreaker() const { return false; }

std::shared_ptr<AbstractOperatorProxy> ExportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  // Extract data members from JSON
  auto bucket_name = json.GetString(kJsonKeyBucketName);
  auto target_object_key = json.GetString(kJsonKeyTargetObjectKey);
  auto export_format = *magic_enum::enum_cast<FileFormat>(json.GetString(kJsonKeyExportFormat));

  auto export_proxy = ExportOperatorProxy::Make(ObjectReference(bucket_name, target_object_key), export_format);
  export_proxy->SetAttributesFromJson(json);

  return export_proxy;
}

Aws::Utils::Json::JsonValue ExportOperatorProxy::ToJson() const {
  return AbstractOperatorProxy::ToJson()
      .WithString(kJsonKeyBucketName, target_object_.bucket_name)
      .WithString(kJsonKeyTargetObjectKey, target_object_.identifier)
      .WithString(kJsonKeyExportFormat, std::string(magic_enum::enum_name(export_format_)));
}

std::shared_ptr<AbstractOperatorProxy> ExportOperatorProxy::Dummy(
    const std::shared_ptr<AbstractOperatorProxy>& input_proxy) {
  auto export_proxy =
      ExportOperatorProxy::Make(ObjectReference(kPlaceholderString, kPlaceholderString), FileFormat::kCsv);
  if (input_proxy) {
    export_proxy->SetLeftInput(input_proxy);
  }
  return export_proxy;
}

std::shared_ptr<AbstractOperatorProxy> ExportOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return ExportOperatorProxy::Make(target_object_, export_format_, copied_left_input);
}

size_t ExportOperatorProxy::ShallowHash() const {
  size_t hash = boost::hash_value(target_object_.bucket_name);
  boost::hash_combine(hash, target_object_.identifier);
  boost::hash_combine(hash, export_format_);

  return hash;
}

std::shared_ptr<AbstractOperator> ExportOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(LeftInput(), "Missing input operator proxy.");
  return std::make_shared<ExportOperator>(LeftInput()->GetOrCreateOperatorInstance(), target_object_.bucket_name,
                                          target_object_.identifier, export_format_);
}

}  // namespace skyrise
