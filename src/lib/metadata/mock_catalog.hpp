#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "abstract_catalog.hpp"

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
  std::shared_ptr<TableSchema> GetTableSchema(const std::string& table_name) const override;

  const std::string& TableBucketName(const std::string& table_name) const override;
  const std::vector<TablePartition>& GetTablePartitions(const std::string& table_name) const override;

 private:
  std::unordered_map<std::string, std::shared_ptr<TableSchema>> table_schema_by_table_name_;
  std::unordered_map<std::string, std::vector<TablePartition>> table_partitions_by_table_name_;
};

}  // namespace skyrise
