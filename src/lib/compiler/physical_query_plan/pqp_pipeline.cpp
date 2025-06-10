#include "pqp_pipeline.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/container_hash/hash.hpp>

#include "pqp_utils.hpp"

namespace skyrise {

PqpPipeline::PqpPipeline(std::string identity, const std::shared_ptr<AbstractOperatorProxy>& pipeline_plan)
    : identity_(std::move(identity)) {
  Assert(!identity_.empty(), "Expected non-empty identity string.");

  // Prefix all operator proxies, so that their future logs and metrics can be associated with this pipeline.
  PrefixOperatorProxyIdentities(pipeline_plan, identity_);
  fragment_template_ = std::make_shared<PipelineFragmentTemplate>(pipeline_plan);
}

const std::string& PqpPipeline::Identity() const { return identity_; }

const std::shared_ptr<PipelineFragmentTemplate>& PqpPipeline::PqpPipelineFragmentTemplate() const {
  return fragment_template_;
}

void PqpPipeline::AddFragmentDefinition(PipelineFragmentDefinition fragment_definition) {
  if constexpr (SKYRISE_DEBUG) {
    for (const auto& import_identity_to_objects : fragment_definition.identity_to_objects) {
      // All operator proxies in the fragment template have already been prefixed with the pipeline's identity.
      // Consequently, a PipelineFragmentDefinition must specify import proxy identities prefixed with the pipeline's
      // identity, so that PipelineFragmentTemplate can map the import definitions accordingly.
      Assert(boost::starts_with(import_identity_to_objects.first, identity_),
             "PipelineFragmentDefinition specifies import proxy identities that are not prefixed with this pipeline's "
             "identity.");
    }
  }
  fragment_definitions_.emplace_back(std::move(fragment_definition));
}

const std::vector<PipelineFragmentDefinition>& PqpPipeline::FragmentDefinitions() const {
  return fragment_definitions_;
}

void PqpPipeline::SetAsPredecessorOf(std::shared_ptr<PqpPipeline> successor_pipeline) {
  if constexpr (SKYRISE_DEBUG) {
    auto iter = std::find_if(successor_pipeline->predecessors_.cbegin(), successor_pipeline->predecessors_.cend(),
                             [this](const auto& existing_predecessor_pipeline) {
                               return existing_predecessor_pipeline.lock().get() == this;
                             });
    Assert(iter == successor_pipeline->predecessors_.cend(), "PqpPipeline is already set as a predecessor!");
  }
  successor_pipeline->predecessors_.emplace_back(weak_from_this());
  successors_.push_back(std::move(successor_pipeline));
}

std::vector<std::weak_ptr<PqpPipeline>> PqpPipeline::Predecessors() const { return predecessors_; }

std::vector<std::shared_ptr<PqpPipeline>> PqpPipeline::Successors() const { return successors_; }

std::ostream& operator<<(std::ostream& stream, const PqpPipeline& pipeline) {
  stream << "Pipeline: " << pipeline.Identity();
  stream << "\n- Predecessor Pipelines:";
  for (const auto& predecessor_pipeline_weak : pipeline.Predecessors()) {
    if (const auto predecessor_pipeline = predecessor_pipeline_weak.lock()) {
      stream << "\n  - " << predecessor_pipeline->Identity();
    } else {
      stream << "\n  - [expired weak pointer]";
    }
  }
  if (pipeline.Predecessors().empty()) {
    stream << "\n  - none";
  }
  stream << "\n- Successor Pipelines:";
  for (const auto& successor_pipeline : pipeline.Successors()) {
    stream << "\n  - " << successor_pipeline->Identity();
  }
  if (pipeline.Successors().empty()) {
    stream << "\n  - none";
  }
  stream << "\n- Fragment Definitions: " << pipeline.FragmentDefinitions().size();
  stream << "\n- Templated Plan:\n";
  stream << *pipeline.PqpPipelineFragmentTemplate()->TemplatedPlan() << '\n';

  return stream;
}

void PqpPipeline::SetSynthetic(bool state) { is_synthetic_pipeline_ = state; }

bool PqpPipeline::IsSynthetic() const { return is_synthetic_pipeline_; }

size_t PqpPipeline::ResultCacheHash(bool skip_synthetic_pipelines) const {
  std::vector<size_t> predecessor_hashes;
  predecessor_hashes.reserve(predecessors_.size());

  for (const auto& predecessor : predecessors_) {
    predecessor_hashes.push_back(predecessor.lock()->ResultCacheHash(skip_synthetic_pipelines));
  }
  std::ranges::sort(predecessor_hashes);

  size_t hash = 0;
  if (IsSynthetic() && skip_synthetic_pipelines) {
    Assert(!predecessors_.empty(), "PqpPipeline cannot be synthetic and without predecessors.");
    hash = predecessor_hashes[0];
    predecessor_hashes.erase(predecessor_hashes.begin());
  } else {
    hash = PqpPipelineFragmentTemplate()->TemplatedPlan()->Hash();
  }

  for (const size_t predecessor_hash : predecessor_hashes) {
    boost::hash_combine(hash, predecessor_hash);
  }

  return hash;
};

}  // namespace skyrise
