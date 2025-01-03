#pragma once

#include "table_partition.hpp"
#include "table_schema.hpp"

namespace skyrise {

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class AbstractCatalog {
 public:
  AbstractCatalog() = default;
  virtual ~AbstractCatalog() = default;

  /**
   * @return true if the catalog contains information about @param table_name.
   */
  virtual bool TableExists(const std::string& table_name) const = 0;

  /**
   * @return a pointer to the TableSchema definition for the given @param table_name.
   * @pre A table with @param table_name must exist.
   */
  virtual std::shared_ptr<const TableSchema> GetTableSchema(const std::string& table_name) const = 0;

  /**
   * @return the name of the bucket in which @param table_name's partitions are stored.
   */
  virtual const std::string& TableBucketName(const std::string& table_name) const = 0;

  /**
   * @return a vector of table partitions belonging to @param table_name.
   */
  virtual const std::vector<TablePartition>& GetTablePartitions(const std::string& table_name) const = 0;
};

}  // namespace skyrise
