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

enum class ExchangeStrategyType {
  kPartialMerge,
  kFullMerge,
  kRepartition,
};

struct ExchangeResult {
  explicit ExchangeResult(
      std::shared_ptr<ImportOperatorProxy> init_next_pipeline_import_proxy,
      std::vector<PipelineFragmentDefinition> init_fragment_definitions,
      std::optional<std::shared_ptr<PartitionOperatorProxy>> init_pipeline_partition_proxy = std::nullopt_t)
      : next_pipeline_import_proxy(std::move(init_next_pipeline_import_proxy)),
        pipeline_partition_proxy(init_pipeline_partition_proxy),
        pipeline_fragment_definitions(std::move(init_fragment_definitions)) {}

  /**
   * TODO(julianmenzler)
   */
  const std::vector<PipelineFragmentDefinition> pipeline_fragment_definitions;

  /**
   * TODO(julianmenzler)
   */
  const std::optional<std::shared_ptr<PartitionOperatorProxy>> pipeline_partition_proxy;


  /**
   * TODO(julianmenzler)
   */
  const std::shared_ptr<ImportOperatorProxy> next_pipeline_import_proxy;
};

class AbstractExchangeStrategy {
 public:
  explicit AbstractExchangeStrategy(const ExchangeStrategyType type);

  const ExchangeStrategyType GetExchangeStrategyType() const;

  virtual size_t OutputObjectsCount(size_t input_objects_count) const;
  virtual size_t OutputPartitionsCount() const = 0;

  virtual ExchangeResult ComputeExchangeResult(
      const size_t pipeline_id, const std::shared_ptr<CompilationContext>& compilation_context,
      const std::vector<std::shared_ptr<ImportOperatorProxy>>& import_proxies) const = 0;

 protected:
  const ExchangeStrategyType type_;
};

class MergeExchangeStrategy : public AbstractExchangeStrategy {
 public:
  explicit MergeExchangeStrategy(size_t output_objects_count);

  size_t OutputObjectsCount(size_t input_objects_count) const override;
  size_t OutputPartitionsCount() const override;

  ExchangeResult ComputeExchangeResult(
      const size_t pipeline_id, const std::shared_ptr<CompilationContext>& compilation_context,
      const std::vector<std::shared_ptr<ImportOperatorProxy>>& import_proxies) const override;

 protected:
  size_t output_objects_count_;
};

}  // namespace skyrise
