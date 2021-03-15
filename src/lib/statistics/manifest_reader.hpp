#pragma once

#include <aws/core/utils/json/JsonSerializer.h>

#include "serialization/schema_serialization.hpp"
#include "statistics/statistics_collector.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/orc.hpp"
#include "utils/assert.hpp"

namespace skyrise {

class ManifestReader {
 public:
  ManifestReader(const std::shared_ptr<Storage>& storage, const std::string& object_identifier);

  size_t GetNumberOfPartitions() const;
  bool HasNextPartition() const;
  ObjectStatistics ReadNextPartition();
  std::string GetManifestVersion();

  std::string GetTablePrefix();
  std::shared_ptr<TableColumnDefinitions> GetOriginalSchema();

 public:
  static constexpr size_t kMaxiumBatchSize = 256LU;

 private:
  void ReconstructStatistics();

  std::unique_ptr<orc::Reader> reader_;
  std::unique_ptr<orc::RowReader> row_reader_;
  std::unique_ptr<orc::ColumnVectorBatch> reader_batch_;
  orc::StructVectorBatch* current_batch_;
  std::vector<ObjectStatistics> parsed_statistics_;

  orc::RowReaderOptions row_reader_options_;
  std::shared_ptr<TableColumnDefinitions> schema_;
  size_t number_of_partitions_ = 0;
  size_t current_partition_index_ = 0;
  size_t current_batch_index_ = 0;
};

}  // namespace skyrise
