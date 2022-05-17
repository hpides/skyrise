#include "export_operator_proxy.hpp"

#include <boost/container_hash/hash.hpp>
#include <magic_enum.hpp>

#include "operator/export_operator.hpp"
#include "utils/json.hpp"

namespace {

const std::string kJsonKeyBucketName = "bucket_name";
const std::string kJsonKeyExportFormat = "export_format";
const std::string kJsonKeyTargetObjectKey = "target_object_key";
const std::string kName = "Export";
const std::string kPlaceholderString = "PLACEHOLDER";

}  // namespace

namespace skyrise {

ExportOperatorProxy::ExportOperatorProxy(std::string bucket_name, std::string target_object_key,
                                         ExportFormat export_format)
    : AbstractOperatorProxy(OperatorType::kExport),
      bucket_name_(std::move(bucket_name)),
      target_object_key_(std::move(target_object_key)),
      export_format_(export_format) {}

const std::string& ExportOperatorProxy::Name() const { return kName; }

std::string ExportOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;
  stream << bucket_name_ << "/";
  if (mode == DescriptionMode::kMultiLine) {
    stream << separator;
  }
  stream << target_object_key_;
  return stream.str();
}

const std::string& ExportOperatorProxy::BucketName() const { return bucket_name_; }

const std::string& ExportOperatorProxy::TargetObjectKey() const { return target_object_key_; }

ExportFormat ExportOperatorProxy::GetExportFormat() const { return export_format_; }

bool ExportOperatorProxy::IsPipelineBreaker() const { return false; }

std::shared_ptr<AbstractOperatorProxy> ExportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  // Extract data members from JSON
  auto bucket_name = json.GetString(kJsonKeyBucketName);
  auto target_object_key = json.GetString(kJsonKeyTargetObjectKey);
  auto export_format = *magic_enum::enum_cast<ExportFormat>(json.GetString(kJsonKeyExportFormat));

  auto export_proxy = ExportOperatorProxy::Make(bucket_name, target_object_key, export_format);
  export_proxy->SetAttributesFromJson(json);

  return export_proxy;
}

Aws::Utils::Json::JsonValue ExportOperatorProxy::ToJson() const {
  Assert(bucket_name_ != kPlaceholderString && target_object_key_ != kPlaceholderString,
         "Did not expect to serialize a dummy Export.");

  return AbstractOperatorProxy::ToJson()
      .WithString(kJsonKeyBucketName, bucket_name_)
      .WithString(kJsonKeyTargetObjectKey, target_object_key_)
      .WithString(kJsonKeyExportFormat, std::string(magic_enum::enum_name(export_format_)));
}

std::shared_ptr<AbstractOperatorProxy> ExportOperatorProxy::DummyExportOperatorProxy() {
  return ExportOperatorProxy::Make(kPlaceholderString, kPlaceholderString, ExportFormat::kOrc);
}

std::shared_ptr<AbstractOperatorProxy> ExportOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  return ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_, copied_left_input);
}

size_t ExportOperatorProxy::ShallowHash() const {
  size_t hash = boost::hash_value(bucket_name_);
  boost::hash_combine(hash, target_object_key_);
  boost::hash_combine(hash, export_format_);

  return hash;
}

std::shared_ptr<AbstractOperator> ExportOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(LeftInput(), "Missing input operator proxy.");
  return std::make_shared<ExportOperator>(LeftInput()->GetOrCreateOperatorInstance(), bucket_name_, target_object_key_,
                                          export_format_);
}

}  // namespace skyrise
