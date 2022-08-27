#pragma once

#include <memory>
#include <string>

#include "compiler/compilation_context.hpp"
#include "compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/pipeline_fragment_template.hpp"
#include "operator/partitioning_function.hpp"
#include "types.hpp"

namespace skyrise {

struct ExchangeResult {
  explicit ExchangeResult(
      std::vector<PipelineFragmentDefinition> init_pipeline_fragment_definitions,
      std::vector<ObjectReference> init_target_objects,
      const size_t init_target_worker_count,
      std::optional<std::shared_ptr<const AbstractPartitioningFunction>> init_pipeline_partitioning_function = std::nullopt)
      : target_partitioning_function(init_pipeline_partitioning_function),
        target_objects(std::move(init_target_objects)),
        pipeline_fragment_definitions(std::move(init_pipeline_fragment_definitions)),
        target_worker_count(init_target_worker_count),

  /**
   * TODO(julianmenzler)
   */
  const std::optional<std::shared_ptr<const AbstractPartitioningFunction>> target_partitioning_function;

  /**
   *
   */
  std::vector<ObjectReference> target_objects;
  std::vector<PipelineFragmentDefinition> pipeline_fragment_definitions;

  /**
   * TODO(julianmenzler)
   */
  const size_t target_worker_count;
};

class AbstractExchangeStrategy {
 public:
  explicit AbstractExchangeStrategy(const ExchangeStrategyType type);
  virtual ~AbstractExchangeStrategy() = default;

  ExchangeStrategyType Type() const;

  size_t Hash() const;

  virtual size_t TargetObjectsCount(size_t input_object_count) const = 0;
  virtual size_t TargetPartitionsCount() const = 0;

  virtual ExchangeResult ComputeExchangeResult(
      const size_t pipeline_id, const std::shared_ptr<CompilationContext>& compilation_context,
      const std::vector<std::shared_ptr<ImportOperatorProxy>>& import_proxies) const = 0;

 protected:
  const ExchangeStrategyType type_;

  virtual size_t ShallowHash() const = 0;
};

}  // namespace skyrise
