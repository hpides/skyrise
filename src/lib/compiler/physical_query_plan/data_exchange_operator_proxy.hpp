#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class DataExchangeOperatorProxy : public EnableMakeForPlanNode<DataExchangeOperatorProxy, AbstractOperatorProxy>,
                                  public AbstractOperatorProxy {
 public:
  DataExchangeOperatorProxy();

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  DataExchangeMode GetDataExchangeMode() const;
  void SetToFullMerge();
  void SetToPartialMerge(size_t output_objects_count);

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;
  size_t OutputObjectsCount() const override;

  // Fails, because it is unsupported.
  Aws::Utils::Json::JsonValue ToJson() const override;

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  DataExchangeMode mode_;
  size_t output_objects_count_;
};

}  // namespace skyrise
