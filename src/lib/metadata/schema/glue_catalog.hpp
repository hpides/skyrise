#pragma once

#include <memory>
#include <unordered_map>

#include "abstract_catalog.hpp"
#include "client/coordinator_client.hpp"
#include "configuration.hpp"
#include "table_schema.hpp"

namespace skyrise {

/**
 * The GlueCatalog provides a schema catalog interface on top of AWS Glue.
 * Each GlueCatalog instance can only work on a single database. For each table in this database, the instance will
 * load and cache the schema and statistics. This means, a specific instance will always return the same table version
 * over its entire lifetime independent of whether this version is still consistent with S3 data or a newer version
 * exists in Glue. In case the up-to-date table version is required, a new GlueCatalog instance needs to be constructed.
 *
 * The current caching strategy optimizes query compilation performance in terms of required request count
 * and data transfer volume.
 */
class GlueCatalog : public AbstractCatalog {
 public:
  GlueCatalog() = delete;
  explicit GlueCatalog(std::shared_ptr<CoordinatorClient> client,
                       std::string database_name = std::string(kDatabaseSchemaName));

  /**
   * In case there is no TableSchema/TableStatistics in the local cache, the latest table version for given
   * @param table_name will be fetched from AWS Glue.
   * Thus, any TableSchema or TableStatistics updates from other GlueCatalog instances will become visible in Glue,
   * but not in this GlueCatalog instance.
   */
  std::shared_ptr<const TableSchema> GetTableSchema(const std::string& table_name) const override;
  const std::vector<TablePartition>& GetTablePartitions(const std::string& table_name) const override;

  /**
   * Adding/Registering a new manifest will cause a table update resulting in a new table version in Glue.
   */
  void AddTableManifest(const std::string& table_name, const ObjectReference& manifest);
  const std::shared_ptr<const ObjectReference>& TableManifest(const std::string& table_name) const;

  const std::string& TableBucketName(const std::string& table_name) const override;
  bool TableExists(const std::string& table_name) const override;

  /**
   * For database and table management.
   */
  std::string GetDatabaseName() const;
  void DeleteDatabase();

  void AddTableMetadata(const std::string& table_name, const std::shared_ptr<const TableSchema>& table_schema,
                        const ObjectReference& metadata);
  void DeleteTableMetadata(const std::string& table_name);
  std::vector<std::string> Tables() const;
  std::vector<Aws::Glue::Model::TableVersion> TableVersions(const std::string& table_name) const;

 private:
  struct TableMetadata {
    const std::string table_name;
    const std::string table_version;
    const std::shared_ptr<const TableSchema> table_schema;
    const std::shared_ptr<const ObjectReference> location;
    const std::shared_ptr<const ObjectReference> manifest;
    std::vector<TablePartition> partitions = {};
  };

  const std::shared_ptr<CoordinatorClient> client_;
  const std::shared_ptr<const Aws::Glue::GlueClient> glue_client_;
  const std::string database_name_;
  mutable std::unordered_map<std::string, std::shared_ptr<TableMetadata>> cache_;

  /**
   * Retrieves and caches the table metadata from Glue.
   */
  const std::shared_ptr<TableMetadata>& LoadTableMetadata(const std::string& table_name) const;
  static std::shared_ptr<TableMetadata> ToTableMetadata(const Aws::Glue::Model::Table& glue_table);
};

}  // namespace skyrise
