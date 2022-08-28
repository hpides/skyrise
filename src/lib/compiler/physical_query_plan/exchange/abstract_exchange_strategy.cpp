#include "abstract_exchange_strategy.hpp"

#include <boost/container_hash/hash.hpp>

namespace skyrise {

ExchangeResult::ExchangeResult(
      std::vector<PipelineFragmentDefinition> init_pipeline_fragment_definitions,
      const size_t init_next_pipeline_target_object_count,
      std::optional<std::shared_ptr<const AbstractPartitioningFunction>> init_pipeline_partitioning_function = std::nullopt)
      : pipeline_partitioning_function(init_pipeline_partitioning_function),
        pipeline_fragment_definitions(std::move(init_pipeline_fragment_definitions)),
        next_pipeline_target_object_count(init_next_pipeline_target_object_count) {}

size_t ExchangeResult::PartitionCount() const {
  return partitioning_function ? *partitioning_function->PartitionCount() : 1;
}

std::vector<ObjectReference> ExchangeResult::ObjectReferences() const {
  Assert(pipeline_fragment_definitions && !pipeline_fragment_definitions.empty(), "Missing PipelineFragmentDefinitions.");

  std::vector<ObjectReference> object_references;
  object_references.reserve(pipeline_fragment_definitions.size());
  for (const auto& fragment_definition : pipeline_fragment_definitions) {
    object_references.push_back(fragment_definition.target_object);
  }

  return object_references;
}

AbstractExchangeStrategy::AbstractExchangeStrategy(const ExchangeStrategyType type) : type_(type) {}

ExchangeStrategyType AbstractExchangeStrategy::Type() const { return type_; }

size_t AbstractExchangeStrategy::Hash() const {
  size_t hash = boost::hash_value(type_);
  boost::hash_combine(hash, ShallowHash());
  return hash;
}

}  // namespace skyrise
