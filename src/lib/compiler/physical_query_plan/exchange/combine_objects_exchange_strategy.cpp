#include "combine_objects_exchange_strategy.hpp"

#include <boost/container_hash/hash.hpp>

namespace skyrise {

CombineObjectsExchangeStrategy::CombineObjectsExchangeStrategy(size_t target_object_count)
    : AbstractExchangeStrategy(ExchangeStrategyType::kCombineObjects),
      target_object_count_(target_object_count) {
  Assert(target_object_count_ > 0, "Cannot combine to zero objects.");
}

size_t CombineObjectsExchangeStrategy::TargetObjectCount(size_t /* input_object_count */) const {
  return target_object_count_;
}

size_t CombineObjectsExchangeStrategy::TargetPartitionCount() const { return 1; }

ExchangeResult CombineObjectsExchangeStrategy::ComputeExchangeResult(
      const size_t /*pipeline_id*/, const std::shared_ptr<CompilationContext>& /*compilation_context*/,
      const std::vector<std::shared_ptr<ImportOperatorProxy>>& /*import_proxies*/) const  {

  /**
   * TODOs
   * 1) Use Interface in ExchangeProxy -> Done.
   * 2) Use Interface in Pipeline Slicer
   * 2) Build ExchangeStrategyType tests
   * 3) Implement this function
   */

  std::vector<PipelineFragmentDefinition> fragment_definitions;
  std::vector<ObjectReference> target_objects;
  const size_t partition_count = 1;
  const size_t target_worker_count = target_object_count_;

  return ExchangeResult(fragment_definitions, target_objects, partition_count, target_worker_count);
}

size_t CombineObjectsExchangeStrategy::ShallowHash() const {
  return boost::hash_value(target_object_count_);
}

}  // namespace skyrise
