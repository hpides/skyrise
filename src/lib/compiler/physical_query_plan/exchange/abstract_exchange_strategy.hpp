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
      size_t init_next_pipeline_target_object_count, // TODO remove this count?
      std::optional<std::shared_ptr<const AbstractPartitioningFunction>> init_pipeline_partitioning_function = std::nullopt);

  size_t PartitionCount() const;
  std::vector<ObjectReference> ObjectReferences() const;

  /**
   * TODO(julianmenzler)
   */
  const std::optional<std::shared_ptr<const AbstractPartitioningFunction>> pipeline_partitioning_function;
  
  /**
   *
   */
  std::vector<PipelineFragmentDefinition> pipeline_fragment_definitions;

  /**
   * TODO(julianmenzler) Worker count
   */
  const size_t next_pipeline_target_object_count;
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
