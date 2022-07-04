#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "operator/partitioning_function.hpp"
#include "types.hpp"

namespace skyrise {

class ExchangeOperatorProxy : public EnableMakeForPlanNode<ExchangeOperatorProxy, AbstractOperatorProxy>,
                              public AbstractOperatorProxy {
 public:
  ExchangeOperatorProxy();

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  ExchangeMode GetExchangeMode() const;
  std::shared_ptr<const AbstractPartitioningFunction> PartitioningFunction() const;
  void SetToFullMerge();
  void SetToPartialMerge(size_t output_objects_count);
  void SetToFullyMeshedExchange(std::shared_ptr<const AbstractPartitioningFunction> partitioning_function);
  void SetToFullyMeshedExchange(std::shared_ptr<const AbstractPartitioningFunction> partitioning_function,
                                size_t output_objects_count);

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;
  size_t OutputObjectsCount() const override;
  size_t OutputPartitionsCount() const override;

  // Fails, because it is unsupported.
  Aws::Utils::Json::JsonValue ToJson() const override;

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  size_t ShallowHash() const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  ExchangeMode mode_;
  size_t output_objects_count_;
  std::shared_ptr<const AbstractPartitioningFunction> partitioning_function_;
};

}  // namespace skyrise
