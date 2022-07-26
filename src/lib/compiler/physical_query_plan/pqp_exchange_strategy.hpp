#pragma once

#include <memory>
#include <string>

#include "operator_proxy/abstract_operator_proxy.hpp"
#include "operator_proxy/import_operator_proxy.hpp"
#include "pipeline_fragment_template.hpp"
#include "types.hpp"

namespace skyrise {

enum class ExchangeStrategyType {
  kPartialMerge,
  kFullMerge,
  kRepartition,
};

struct ExchangeResult {
  const size_t output_partition_count;
  const std::vector<ObjectReference> output_object_references;
  const std::vector<PipelineFragmentDefinition> fragment_definitions;
};

class AbstractExchangeStrategy {
 public:
  explicit AbstractExchangeStrategy(const ExchangeStrategyType type);

  const ExchangeStrategyType GetExchangeStrategyType() const;

  virtual size_t OutputObjectsCount(size_t input_objects_count) = 0;
  virtual size_t OutputPartitionsCount() = 0;

  virtual std::vector<PipelineFragmentDefinition> GeneratePipelineFragmentDefinitions(
      std::shared_ptr<CompilationContext> compilation_context, std::vector<std::shared_ptr<ImportOperatorProxy>> import_proxies);

 protected:
  ExchangeStrategyType type_;
};

}  // namespace skyrise
