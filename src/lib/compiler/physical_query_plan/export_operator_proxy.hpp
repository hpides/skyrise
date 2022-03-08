#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"

namespace skyrise {

enum class ExportFormat { kCsv, kOrc, kOrcPartitioned };

class ExportOperatorProxy : public EnableMakeForPlanNode<ExportOperatorProxy, AbstractOperatorProxy>,
                            public AbstractOperatorProxy {
 public:
  ExportOperatorProxy(std::string bucket_name, std::string target_object_key, ExportFormat export_format);

  const std::string& Name() const override;
  std::string Description(const DescriptionMode mode) const override;

  /**
   * Accessors
   */
  const std::string& BucketName() const;
  const std::string& TargetObjectKey() const;
  ExportFormat GetExportFormat() const;

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
   * Convenience construction function:
   * @returns an ExportOperatorProxy without proper values for bucket name etc.
   */
  static std::shared_ptr<AbstractOperatorProxy> DummyExportOperatorProxy();

 protected:
  std::shared_ptr<AbstractOperatorProxy> OnDeepCopy(
      const std::shared_ptr<AbstractOperatorProxy>& copied_left_input,
      const std::shared_ptr<AbstractOperatorProxy>& copied_right_input) const override;
  std::shared_ptr<AbstractOperator> CreateOperatorInstanceRecursively() override;

 private:
  std::string bucket_name_;
  std::string target_object_key_;
  ExportFormat export_format_;
};

}  // namespace skyrise
