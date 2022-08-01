#pragma once

#include <memory>
#include <optional>
#include <string>

#include "compiler/compilation_context.hpp"
#include "operator_proxy/abstract_operator_proxy.hpp"
#include "operator_proxy/import_operator_proxy.hpp"
#include "operator_proxy/partition_operator_proxy.hpp"
#include "pipeline_fragment_template.hpp"
#include "types.hpp"

namespace skyrise {

struct ExchangeResult {
  explicit ExchangeResult(
      std::vector<PipelineFragmentDefinition> init_pipeline_fragment_definitions,
      std::vector<ObjectReference> init_target_objects,
      const size_t init_target_partition_count,
      const size_t init_target_worker_count,
      std::optional<std::shared_ptr<PartitionOperatorProxy>> init_pipeline_partition_proxy = std::nullopt)
      : pipeline_fragment_definitions(init_pipeline_fragment_definitions), target_objects(init_target_objects),
        target_partition_count(init_target_partition_count), target_worker_count(init_target_worker_count),
        pipeline_partition_proxy(init_pipeline_partition_proxy) {}

  /**
   * TODO(julianmenzler)
   */
  std::vector<PipelineFragmentDefinition> pipeline_fragment_definitions;
  std::vector<ObjectReference> target_objects;
  const size_t target_partition_count;
  const size_t target_worker_count;

  /**
   * TODO(julianmenzler)
   */
  std::optional<std::shared_ptr<PartitionOperatorProxy>> pipeline_partition_proxy;
};

class AbstractExchangeStrategy {
 public:
  explicit AbstractExchangeStrategy(const ExchangeStrategyType type);

  ExchangeStrategyType Type() const;

  size_t Hash();
  virtual std::unique_ptr<AbstractExchangeStrategy> DeepCopy() const = 0;

  virtual size_t TargetObjectCount(size_t input_object_count) const;
  virtual size_t TargetPartitionCount() const = 0;

  virtual ExchangeResult ComputeExchangeResult(
      const size_t pipeline_id, const std::shared_ptr<CompilationContext>& compilation_context,
      const std::vector<std::shared_ptr<ImportOperatorProxy>>& import_proxies) const = 0;

 protected:
  const ExchangeStrategyType type_;

  virtual size_t ShallowHash() = 0;
};

class CombineObjectsExchangeStrategy : public AbstractExchangeStrategy {
 public:
  explicit CombineObjectsExchangeStrategy(size_t target_object_count);

  std::unique_ptr<AbstractExchangeStrategy> DeepCopy() const override;

  size_t TargetObjectCount(size_t input_object_count) const override;
  size_t TargetPartitionCount() const override;

  ExchangeResult ComputeExchangeResult(
      const size_t pipeline_id, const std::shared_ptr<CompilationContext>& compilation_context,
      const std::vector<std::shared_ptr<ImportOperatorProxy>>& import_proxies) const override;

 protected:
  size_t target_object_count_;
};

}  // namespace skyrise
