#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "import_options.hpp"
#include "storage/table/chunk_reader.hpp"
#include "storage/table/table_column_definition.hpp"

namespace skyrise {

class ImportOperatorProxy : public AbstractOperatorProxy {
 public:
  ImportOperatorProxy(std::string bucket_name, std::vector<std::string> object_keys, std::vector<ColumnId> column_ids);

  const std::string& Name() const override;

  const std::string& BucketName() const;
  const std::vector<std::string>& ObjectKeys() const;
  const std::vector<ColumnId>& ColumnIds() const;

  // If desired, non-default options for reading CSV/ORC data can be set.
  void SetImportOptions(std::shared_ptr<const ImportOptions> import_options);
  std::shared_ptr<const ImportOptions> GetImportOptions() const;

  /**
   * Serialization / Deserialization
   */
  Aws::Utils::Json::JsonValue ToJson() const override;
  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json);

 protected:
  std::shared_ptr<AbstractOperator> CreateOperatorInstance() override;

 private:
  const std::string bucket_name_;
  const std::vector<std::string> object_keys_;
  const std::vector<ColumnId> column_ids_;
  std::shared_ptr<const ImportOptions> import_options_;
};

}  // namespace skyrise
