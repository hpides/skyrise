#pragma once

#include <memory>
#include <string>

#include "abstract_operator_proxy.hpp"
#include "storage/table/chunk_reader.hpp"
#include "storage/table/table_column_definition.hpp"

namespace skyrise {

class ImportOperatorProxy : public AbstractOperatorProxy {
 public:
  enum class ObjectFormat : uint8_t { kCsv, kOrc };
  ImportOperatorProxy(std::string bucket_name, std::vector<std::string> objects_keys, std::vector<ColumnId> column_ids,
                      ObjectFormat format, std::shared_ptr<AbstractChunkReaderFactory> reader_factory);

  const std::string& Name() const override;

  static std::shared_ptr<AbstractOperatorProxy> FromJson(const Aws::Utils::Json::JsonView& json,
                                                         StorageFactory storage_factory = nullptr);
  Aws::Utils::Json::JsonValue ToJson() const override;

  void SetStorageFactory(StorageFactory storage_factory);

 protected:
  std::shared_ptr<AbstractOperator> CreateOperatorInstance() const override;
  static std::shared_ptr<TableColumnDefinitions> ParseColumnDefinitions(const Aws::Utils::Json::JsonView json);
  static Aws::Utils::Array<Aws::Utils::Json::JsonValue> WriteColumnDefinitions(
      const TableColumnDefinitions& definitions);

 private:
  const std::string bucket_name_;
  const std::vector<std::string> objects_keys_;
  const std::vector<ColumnId> column_ids_;
  const ObjectFormat format_;
  const std::shared_ptr<AbstractChunkReaderFactory> reader_factory_;
  StorageFactory storage_factory_;
};

}  // namespace skyrise
