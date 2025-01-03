#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class SortOperatorProxy : public EnableMakeForPlanNode<SortOperatorProxy, AbstractOperatorProxy>,
                          public AbstractOperatorProxy {
 public:
  explicit SortOperatorProxy(std::vector<SortColumnDefinition> sort_definitions);

  const std::string& Name() const override;

  std::vector<SortColumnDefinition> SortDefinitions() const;

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;

  /**
   * Serialization / Deserialization
   */
  Aws::Utils::Json::JsonValue ToJson() const override;
  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  size_t ShallowHash() const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  const std::vector<SortColumnDefinition> sort_definitions_;
};

}  // namespace skyrise
