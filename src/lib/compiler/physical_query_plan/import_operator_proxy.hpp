#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"

namespace skyrise {

class ImportOperatorProxy : public AbstractOperatorProxy {
 public:
  enum class ObjectFormat : uint8_t { kCsv, kOrc };
  ImportOperatorProxy(std::string bucket_name, std::vector<std::string> objects_keys,
                      std::vector<ColumnId> pruned_column_ids, ObjectFormat format);

  const std::string& Name() const override;

  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json,
                                                         StorageFactory storage_factory = nullptr);
  Aws::Utils::Json::JsonValue ToJson() const override;

  void SetStorageFactory(StorageFactory storage_factory);

 protected:
  std::shared_ptr<AbstractOperator> CreateOperatorInstance() const override;

 private:
  const std::string bucket_name_;
  const std::vector<std::string> objects_keys_;
  const std::vector<ColumnId> pruned_column_ids_;
  const ObjectFormat format_;
  StorageFactory storage_factory_;
};

}  // namespace skyrise
