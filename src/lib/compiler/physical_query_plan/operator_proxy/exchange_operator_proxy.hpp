#pragma once

#include <memory>
#include <optional>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "operator/partitioning_function.hpp"
#include "types.hpp"

namespace skyrise {

class ExchangeOperatorProxy : public EnableMakeForPlanNode<ExchangeOperatorProxy, AbstractOperatorProxy>,
                              public AbstractOperatorProxy {
 public:
  ExchangeOperatorProxy(
      ExchangeType exchange_type, size_t target_bucket_count,
      std::optional<std::shared_ptr<const AbstractPartitioningFunction>> target_partitioning_function);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  ExchangeType GetExchangeType() const;
  size_t TargetBucketCount() const;
  const std::optional<std::shared_ptr<const AbstractPartitioningFunction>>& TargetPartitioningFunction() const;

  /**
   * Optimization-relevant attributes
   */
  const DataTraits& OutputDataTraits() const override;
  bool IsPipelineBreaker() const override;

  // Fails, because it is unsupported.
  Aws::Utils::Json::JsonValue ToJson() const override;

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  size_t ShallowHash() const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  const ExchangeType exchange_type_;
  const size_t target_bucket_count_;
  const std::optional<std::shared_ptr<const AbstractPartitioningFunction>> target_partitioning_function_;

  // Mutable because the data structure is refreshed in the Getter to align with InputDataTraits.
  mutable DataTraits output_data_traits_;
};

}  // namespace skyrise
