#pragma once

#include <memory>
#include <string>

#include "operator_proxy/abstract_operator_proxy.hpp"
#include "pipeline_fragment_template.hpp"
#include "types.hpp"

namespace skyrise {

class PqpPipeline : public std::enable_shared_from_this<PqpPipeline>, public Noncopyable {
 public:
  PqpPipeline(std::string pipeline_identity, const std::shared_ptr<AbstractOperatorProxy>& pipeline_plan);

  /**
   * @returns an identity string unique to this PqpPipeline.
   */
  const std::string& Identity() const;

  const std::shared_ptr<PipelineFragmentTemplate>& FragmentTemplate() const;

  void AddFragmentDefinition(PipelineFragmentDefinition fragment_definition);
  const std::vector<PipelineFragmentDefinition>& FragmentDefinitions() const;

  // Relationship Management
  void SetAsPredecessorOf(std::shared_ptr<PqpPipeline> successor_pipeline);
  std::vector<std::weak_ptr<PqpPipeline>> Predecessors() const;
  std::vector<std::shared_ptr<PqpPipeline>> Successors() const;

 protected:
  const std::string identity_;
  std::shared_ptr<PipelineFragmentTemplate> fragment_template_;
  std::vector<PipelineFragmentDefinition> fragment_definitions_;

  std::vector<std::weak_ptr<PqpPipeline>> predecessors_;
  std::vector<std::shared_ptr<PqpPipeline>> successors_;
};

std::ostream& operator<<(std::ostream& stream, const PqpPipeline& pipeline);

}  // namespace skyrise
