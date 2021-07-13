#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"

namespace skyrise {

class ExportOperatorProxy : public AbstractOperatorProxy {
 public:
  enum class ObjectFormat { kCsv, kOrc };

  ExportOperatorProxy(std::string bucket_name, std::string target_object_key,
                      const std::shared_ptr<const AbstractOperatorProxy>& left = nullptr,
                      const std::shared_ptr<const AbstractOperatorProxy>& right = nullptr);

  const std::string& Name() const override;

  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json,
                                                         StorageFactory storage_factory = nullptr);
  Aws::Utils::Json::JsonValue ToJson() const override;

  void SetStorageFactory(StorageFactory storage_factory);

 protected:
  std::shared_ptr<AbstractOperator> CreateOperatorInstance() const override;

 private:
  const std::string bucket_name_;
  StorageFactory storage_factory_;
  const std::string target_object_key_;
};

}  // namespace skyrise
