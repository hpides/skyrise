#include "pipeline_fragment_template.hpp"

#include <magic_enum/magic_enum.hpp>

#include "operator_proxy/export_operator_proxy.hpp"
#include "operator_proxy/import_operator_proxy.hpp"
#include "pqp_utils.hpp"

namespace {

using namespace skyrise;  // NOLINT(google-build-using-namespace)

const ObjectReference kTargetObjectPlaceholder("Placeholder", "Placeholder");

}  // namespace

namespace skyrise {

PipelineFragmentDefinition::PipelineFragmentDefinition(
    std::unordered_map<std::string, std::vector<ObjectReference>> init_identity_to_objects,
    ObjectReference init_target_object, FileFormat init_target_format)
    : identity_to_objects(std::move(init_identity_to_objects)),
      target_object(std::move(init_target_object)),
      target_format(init_target_format) {
  if constexpr (SKYRISE_DEBUG) {
    // Validate input
    Assert(!identity_to_objects.empty(), "At least one import definition must be specified.");
    for (const auto& [identity, object_references] : identity_to_objects) {
      Assert(!identity.empty(), "For import proxy mapping, non-empty identity strings must be provided.");
      Assert(!object_references.empty(), "At least one import definition must be provided.");
    }
    Assert(!target_object.bucket_name.empty() && !target_object.identifier.empty(),
           "Incomplete ObjectReference for target object.");
  }
}

bool PipelineFragmentDefinition::operator==(const PipelineFragmentDefinition& rhs) const {
  return target_format == rhs.target_format && target_object == rhs.target_object &&
         identity_to_objects == rhs.identity_to_objects;
}

PipelineFragmentDefinition PipelineFragmentDefinition::FromJson(const Aws::Utils::Json::JsonView& json) {
  const auto bucket = json.GetString("source_bucket");
  const auto prefix = json.GetString("source_prefix");
  const auto partitions = json.KeyExists("source_partitions")
                              ? std::make_optional(JsonArrayToVector<int32_t>(json.GetArray("source_partitions")))
                              : std::nullopt;
  std::unordered_map<std::string, std::vector<ObjectReference>> id_to_objects;
  const auto json_id_to_objects = json.GetObject("source_objects").GetAllObjects();
  for (const auto& [id, json_objects] : json_id_to_objects) {
    for (size_t i = 0; i < json_objects.AsArray().GetLength(); ++i) {
      const auto identifier = prefix + "/" + json_objects.AsArray().GetItem(i).GetString("source_suffix");
      const auto etag = json_objects.AsArray().GetItem(i).GetString("etag");
      id_to_objects[id].push_back(ObjectReference(bucket, identifier, etag, partitions));
    }
  }
  return {id_to_objects, ObjectReference::FromJson(json.GetObject("target_object")),
          magic_enum::enum_cast<FileFormat>(json.GetString("target_format")).value()};
}

Aws::Utils::Json::JsonValue PipelineFragmentDefinition::ToJson() const {
  std::string bucket;
  std::string prefix;
  std::optional<std::vector<int32_t>> partitions = std::nullopt;

  Aws::Utils::Json::JsonValue inputs;
  for (const auto& [identity, objects] : identity_to_objects) {
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> objects_array(objects.size());
    for (size_t i = 0; i < objects.size(); ++i) {
      size_t last_separator_position = objects[i].identifier.find_last_of('/');
      // Assume organization along pipelines.
      size_t second_to_last_separator_position =
          objects[i].identifier.substr(0, last_separator_position).find_last_of('/');
      if (i == 0) {
        bucket = objects[i].bucket_name;
        prefix = objects[i].identifier.substr(0, second_to_last_separator_position);
        partitions = objects[i].partitions;
      }
      std::string suffix =
          objects[i].identifier.substr(second_to_last_separator_position + 1, objects[i].identifier.size());
      std::string etag = objects[i].etag;
      objects_array[i] = Aws::Utils::Json::JsonValue().WithString("source_suffix", suffix).WithString("etag", etag);
    }
    inputs.WithArray(identity, objects_array);
  }

  auto json = Aws::Utils::Json::JsonValue()
                  .WithString("source_bucket", bucket)
                  .WithString("source_prefix", prefix)
                  .WithObject("source_objects", inputs)
                  .WithObject("target_object", target_object.ToJson())
                  .WithString("target_format", std::string(magic_enum::enum_name(target_format)));
  if (partitions) {
    json = json.WithArray("source_partitions", VectorToJsonArray(*partitions));
  }
  return json;
}

PipelineFragmentTemplate::PipelineFragmentTemplate(const std::shared_ptr<AbstractOperatorProxy>& pipeline_plan) {
  Assert(pipeline_plan->Type() == OperatorType::kExport, "Expected export proxy as the root of the fragment template.");

  // Create a deep copy, so that the templated plan cannot be modified from outside this class.
  auto mutable_template = pipeline_plan->DeepCopy();

  // Clear Export fields
  const auto export_proxy = std::static_pointer_cast<ExportOperatorProxy>(mutable_template);
  export_proxy->SetTargetObject(kTargetObjectPlaceholder, FileFormat::kOrc);

  // Clear Import fields
  auto leaf_proxies = PqpFindLeaves(std::const_pointer_cast<AbstractOperatorProxy>(mutable_template));
  for (const auto& leaf_proxy : leaf_proxies) {
    Assert(leaf_proxy->Type() == OperatorType::kImport,
           "Expected import proxy as a leaf node in the fragment template.");
    const auto import_proxy = std::static_pointer_cast<ImportOperatorProxy>(leaf_proxy);
    import_proxy->SetObjectReferences(std::vector<ObjectReference>());
  }

  template_ = mutable_template;
}

std::shared_ptr<AbstractOperatorProxy> PipelineFragmentTemplate::GenerateFragmentPlan(
    const PipelineFragmentDefinition& fragment_definition) const {
  auto fragment_instance = template_->DeepCopy();

  // Configure Export
  auto export_proxy = std::static_pointer_cast<ExportOperatorProxy>(fragment_instance);
  export_proxy->SetTargetObject(fragment_definition.target_object, fragment_definition.target_format);

  // Configure Imports
  auto leaf_proxies = PqpFindLeaves(fragment_instance);
  Assert(leaf_proxies.size() == fragment_definition.identity_to_objects.size(),
         "Number of import identities is not equal to the number of import proxy leaves in the fragment template.");

  for (const auto& leaf_proxy : leaf_proxies) {
    auto import_proxy = std::static_pointer_cast<ImportOperatorProxy>(leaf_proxy);
    const auto object_references_iter = fragment_definition.identity_to_objects.find(import_proxy->Identity());
    Assert(object_references_iter != fragment_definition.identity_to_objects.cend(),
           "Did not find ObjectReference for given import proxy.");
    import_proxy->SetObjectReferences(object_references_iter->second);
    import_proxy->SetOutputObjectsCount(1);
  }

  return fragment_instance;
}

std::shared_ptr<const AbstractOperatorProxy> PipelineFragmentTemplate::TemplatedPlan() const { return template_; }

}  // namespace skyrise
