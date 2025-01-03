#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "metadata/schema/abstract_catalog.hpp"

namespace skyrise {

class MockCatalog : public AbstractCatalog {
 public:
  MockCatalog() = default;

  /**
   * Adds @param table_name with @param table_schema to the catalog.
   */
  void AddTableSchema(const std::string& table_name, const std::shared_ptr<TableSchema>& table_schema);

  /**
   * Adds @param table_name to the catalog after extracting its TableSchema from @param file_name's header.
   */
  void AddTableSchemaFromFileHeader(const std::string& table_name, const std::string& file_name);

  bool TableExists(const std::string& table_name) const override;
  std::shared_ptr<TableSchema> EditableTableSchema(const std::string& table_name);
  std::shared_ptr<const TableSchema> GetTableSchema(const std::string& table_name) const override;

  const std::string& TableBucketName(const std::string& table_name) const override;
  const std::vector<TablePartition>& GetTablePartitions(const std::string& table_name) const override;

 private:
  std::unordered_map<std::string, std::shared_ptr<TableSchema>> table_name_to_table_schema_;
  std::unordered_map<std::string, const std::vector<TablePartition>> table_name_to_table_partitions_;
};

}  // namespace skyrise
