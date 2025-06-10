#include "table_partition.hpp"

#include "utils/assert.hpp"

namespace skyrise {

TablePartition::TablePartition(std::string object_key, std::string etag, size_t size, time_t last_modified_timestamp)
    : object_key_(std::move(object_key)),
      etag_(std::move(etag)),
      size_(size),
      last_modified_timestamp_(last_modified_timestamp) {}

const std::string& TablePartition::ObjectKey() const { return object_key_; }

const std::string& TablePartition::ObjectEtag() const { return etag_; }

size_t TablePartition::ObjectSize() const { return size_; }

time_t TablePartition::LastModifiedTimestamp() const { return last_modified_timestamp_; }

TablePartition TablePartition::FromObjectStatus(const ObjectStatus& object_status) {
  Assert(!object_status.GetError().IsError(), "ObjectStatus states errors.");
  return {object_status.GetIdentifier(), object_status.GetChecksum(), object_status.GetSize(),
          object_status.GetLastModifiedTimestamp()};
}

}  // namespace skyrise
