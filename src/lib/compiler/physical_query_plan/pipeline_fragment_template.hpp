#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "operator_proxy/abstract_operator_proxy.hpp"
#include "operator_proxy/export_operator_proxy.hpp"
#include "operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

struct PipelineFragmentDefinition final {
  PipelineFragmentDefinition(std::unordered_map<std::string, std::vector<ObjectReference>> init_identity_to_objects,
                             ObjectReference init_target_object, FileFormat init_target_format);

  bool operator==(const PipelineFragmentDefinition& rhs) const;

  static PipelineFragmentDefinition FromJson(const Aws::Utils::Json::JsonView& json);
  Aws::Utils::Json::JsonValue ToJson() const;

  std::unordered_map<std::string, std::vector<ObjectReference>> identity_to_objects;
  ObjectReference target_object;
  FileFormat target_format;
};

class PipelineFragmentTemplate : public Noncopyable {
 public:
  explicit PipelineFragmentTemplate(const std::shared_ptr<AbstractOperatorProxy>& pipeline_plan);

  std::shared_ptr<AbstractOperatorProxy> GenerateFragmentPlan(
      const PipelineFragmentDefinition& fragment_definition) const;
  std::shared_ptr<const AbstractOperatorProxy> TemplatedPlan() const;

 private:
  std::shared_ptr<const AbstractOperatorProxy> template_;
};

}  // namespace skyrise
