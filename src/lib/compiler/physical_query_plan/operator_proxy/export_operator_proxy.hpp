#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class ExportOperatorProxy : public EnableMakeForPlanNode<ExportOperatorProxy, AbstractOperatorProxy>,
                            public AbstractOperatorProxy {
 public:
  ExportOperatorProxy(ObjectReference target_object, FileFormat export_format);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  void SetTargetObject(ObjectReference target_object, FileFormat export_format);
  const ObjectReference& TargetObject() const;
  FileFormat GetExportFormat() const;

  /**
   * Optimization-relevant attributes
   */
  bool IsPipelineBreaker() const override;

  /**
   * Serialization / Deserialization
   */
  Aws::Utils::Json::JsonValue ToJson() const override;
  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);

  /**
   * Convenience construction function used by PipelineFragmentTemplate.
   * @return an ExportOperatorProxy without proper values for bucket name etc. If @param input_proxy is provided, sets
   *         it is set as an input of the export proxy.
   */
  static std::shared_ptr<AbstractOperatorProxy> Dummy(
      const std::shared_ptr<AbstractOperatorProxy>& input_proxy = nullptr);

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  size_t ShallowHash() const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  ObjectReference target_object_;
  FileFormat export_format_;
};

}  // namespace skyrise
