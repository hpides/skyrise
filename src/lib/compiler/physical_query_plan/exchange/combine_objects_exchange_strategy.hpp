#pragma once

#include <memory>
#include <string>

#include "abstract_exchange_strategy.hpp"
#include "compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

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

  size_t ShallowHash() const override;
};

}  // namespace skyrise
