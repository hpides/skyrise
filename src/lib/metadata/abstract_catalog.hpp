#pragma once

#include "table_partition.hpp"
#include "table_schema.hpp"

namespace skyrise {

class AbstractCatalog {
 public:
  AbstractCatalog() = default;
  virtual ~AbstractCatalog() {}

  /**
   * @returns true if the catalog contains information about @param table_name.
   */
  virtual bool TableExists(const std::string& table_name) const = 0;

  /**
   * @returns a pointer to the TableSchema definition for the given @param table_name.
   * @pre A table with @param table_name must exist.
   */
  virtual std::shared_ptr<TableSchema> GetTableSchema(const std::string& table_name) const = 0;

  /**
   * @returns the name of the bucket in which @param table_name's partitions are stored.
   */
  virtual const std::string& TableBucketName(const std::string& table_name) const = 0;

  /**
   * @returns a vector of table partitions belonging to @param table_name.
   */
  virtual const std::vector<TablePartition>& GetTablePartitions(const std::string& table_name) const = 0;
};

}  // namespace skyrise
