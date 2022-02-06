#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "operator/export_operator.hpp"

namespace skyrise {

class ExportOperatorProxy : public AbstractOperatorProxy {
 public:
  ExportOperatorProxy(std::string bucket_name, std::string target_object_key,
                      ExportOperator::OutputFormat output_format,
                      const std::shared_ptr<AbstractOperatorProxy>& left = nullptr,
                      const std::shared_ptr<AbstractOperatorProxy>& right = nullptr);

  const std::string& Name() const override;

  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);
  Aws::Utils::Json::JsonValue ToJson() const override;

 protected:
  std::shared_ptr<AbstractOperator> CreateOperatorInstance() override;

 private:
  const std::string bucket_name_;
  const std::string target_object_key_;
  const ExportOperator::OutputFormat output_format_;
};

}  // namespace skyrise
