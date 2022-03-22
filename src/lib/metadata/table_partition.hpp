#pragma once

#include <ctime>
#include <string>

#include "storage/backend/abstract_storage.hpp"

namespace skyrise {

/**
 * Table data is usually stored as a compound of different partitions resp. objects in an Amazon S3 bucket.
 * TablePartition wraps the relevant metadata of a single partition object. It is used in, for example, the catalog or
 * physical query plans.
 */
class TablePartition {
 public:
  TablePartition(std::string object_key, std::string etag, size_t size, time_t last_modified_timestamp);

  /**
   * @return the S3 object key of the table partition.
   */
  const std::string& ObjectKey() const;

  /**
   * @return the S3 object ETag resp. checksum of the partition.
   */
  const std::string& ObjectEtag() const;

  /**
   * @return the S3 object's size in bytes.
   */
  size_t ObjectSize() const;

  /**
   * @return the last-modified timestamp.
   */
  time_t LastModifiedTimestamp() const;

  /**
   * @return a new TablePartition from the data of @param object_status.
   */
  static TablePartition FromObjectStatus(const ObjectStatus& object_status);

 private:
  const std::string object_key_;
  const std::string etag_;
  const size_t size_;
  time_t last_modified_timestamp_;
};

}  // namespace skyrise
