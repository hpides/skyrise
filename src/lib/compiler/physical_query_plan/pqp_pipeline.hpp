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

  const std::shared_ptr<PipelineFragmentTemplate>& PqpPipelineFragmentTemplate() const;

  void AddFragmentDefinition(PipelineFragmentDefinition fragment_definition);
  const std::vector<PipelineFragmentDefinition>& FragmentDefinitions() const;

  // Relationship Management
  void SetAsPredecessorOf(std::shared_ptr<PqpPipeline> successor_pipeline);
  std::vector<std::weak_ptr<PqpPipeline>> Predecessors() const;
  std::vector<std::shared_ptr<PqpPipeline>> Successors() const;

  /**
   * @returns a hash that can be employed to identify pipeline overlap with the query result cache.
   *
   * This hash incorporates the hashes of the preceding pipelines for result correctness. The hash specifically excludes
   * the pipeline identity and fragment definitions, because they potentially differ between query runs. This dependency
   * has to be managed by the result cache catalog as they depend on execution constraints and timestamps, leading to
   * varying import/export keys and varying numbers of synthetic pipelines for the same query results. It further
   * optionally excludes these synthetic pipelines introduced during query optimization.
   */
  size_t ResultCacheHash(bool skip_synthetic_pipelines = false) const;

  /**
   * Query optimization may introduce additional, synthetic pipelines to combine or shuffle intermediate results.
   */
  void SetSynthetic(bool state);
  bool IsSynthetic() const;

 protected:
  const std::string identity_;
  std::shared_ptr<PipelineFragmentTemplate> fragment_template_;
  std::vector<PipelineFragmentDefinition> fragment_definitions_;

  std::vector<std::weak_ptr<PqpPipeline>> predecessors_;
  std::vector<std::shared_ptr<PqpPipeline>> successors_;

  bool is_synthetic_pipeline_ = false;
};

std::ostream& operator<<(std::ostream& stream, const PqpPipeline& pipeline);

}  // namespace skyrise
