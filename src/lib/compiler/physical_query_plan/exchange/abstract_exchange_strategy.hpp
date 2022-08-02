#pragma once

#include <memory>
#include <string>

#include "compiler/compilation_context.hpp"
#include "compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "exchange_result.hpp"
#include "types.hpp"

namespace skyrise {

class AbstractExchangeStrategy {
 public:
  explicit AbstractExchangeStrategy(const ExchangeStrategyType type);
  virtual ~AbstractExchangeStrategy() = default;

  ExchangeStrategyType Type() const;

  size_t Hash();
  virtual std::shared_ptr<const AbstractExchangeStrategy> DeepCopy() const = 0;

  virtual size_t TargetObjectCount(size_t input_object_count) const;
  virtual size_t TargetPartitionCount() const = 0;

  virtual ExchangeResult ComputeExchangeResult(
      const size_t pipeline_id, const std::shared_ptr<CompilationContext>& compilation_context,
      const std::vector<std::shared_ptr<ImportOperatorProxy>>& import_proxies) const = 0;

 protected:
  const ExchangeStrategyType type_;

  virtual size_t ShallowHash() const = 0;
};

}  // namespace skyrise
