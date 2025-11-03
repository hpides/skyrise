#include "pipeline_fragment_template.hpp"

#include "operator_proxy/export_operator_proxy.hpp"
#include "operator_proxy/import_operator_proxy.hpp"
#include "pqp_utils.hpp"

namespace {

using namespace skyrise;  // NOLINT(google-build-using-namespace)

const ObjectReference kTargetObjectPlaceholder("Placeholder", "Placeholder");

}  // namespace

namespace skyrise {

PipelineFragmentDefinition::PipelineFragmentDefinition(
    std::unordered_map<std::string, std::vector<ObjectReference>> init_identity_to_object_references,
    ObjectReference init_target_object, ExportFormat init_target_format)
    : identity_to_object_references(std::move(init_identity_to_object_references)),
      target_object(std::move(init_target_object)),
      target_format(init_target_format) {
  if constexpr (SKYRISE_DEBUG) {
    // Validate input
    Assert(!identity_to_object_references.empty(), "At least one import definition must be specified.");
    for (const auto& [identity, object_references] : identity_to_object_references) {
      Assert(!identity.empty(), "For import proxy mapping, non-empty identity strings must be provided.");
      Assert(!object_references.empty(), "At least one import defintion must be provided.");
    }
    Assert(!target_object.bucket_name.empty() && !target_object.identifier.empty(),
           "Incomplete ObjectReference for target object.");
  }
}

bool PipelineFragmentDefinition::operator==(const PipelineFragmentDefinition& rhs) const {
  return target_format == rhs.target_format && target_object == rhs.target_object &&
         identity_to_object_references == rhs.identity_to_object_references;
}

PipelineFragmentTemplate::PipelineFragmentTemplate(const std::shared_ptr<AbstractOperatorProxy>& pipeline_plan) {
  Assert(pipeline_plan->Type() == OperatorType::kExport, "Expected export proxy as the root of the fragment template.");

  // Create a deep copy, so that the templated plan cannot be modified from outside this class.
  auto mutable_template = pipeline_plan->DeepCopy();

  // Clear Export fields
  const auto export_proxy = std::static_pointer_cast<ExportOperatorProxy>(mutable_template);
  export_proxy->SetTargetObject(kTargetObjectPlaceholder, ExportFormat::kOrc);

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
  Assert(leaf_proxies.size() == fragment_definition.identity_to_object_references.size(),
         "Number of import identities is not equal to the number of import proxy leaves in the fragment template.");

  for (const auto& leaf_proxy : leaf_proxies) {
    auto import_proxy = std::static_pointer_cast<ImportOperatorProxy>(leaf_proxy);

    const auto object_references_iter =
        fragment_definition.identity_to_object_references.find(import_proxy->Identity());
    Assert(object_references_iter != fragment_definition.identity_to_object_references.cend(),
           "Did not find ObjectReference for given import proxy.");
    import_proxy->SetObjectReferences(object_references_iter->second);
    import_proxy->SetOutputObjectsCount(1);
  }

  return fragment_instance;
}

std::shared_ptr<const AbstractOperatorProxy> PipelineFragmentTemplate::TemplatedPlan() const { return template_; }

}  // namespace skyrise
