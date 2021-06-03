#pragma once

#include "statistics/statistics_collector.hpp"
#include "storage/backend/abstract_storage.hpp"

namespace skyrise {

class ManifestReader : OrcFormatReader {
 public:
  ManifestReader(std::unique_ptr<ObjectReader> source);
  size_t GetNumberOfPartitions() const;
  bool HasNextPartition();
  ObjectStatistics ReadNextPartition();

  std::string GetManifestVersion();
  std::string GetTablePrefix();
  std::shared_ptr<const TableColumnDefinitions> GetOriginalSchema();

 private:
  void ReconstructStatisticsFromChunk(std::unique_ptr<Chunk> chunk);

  std::vector<ObjectStatistics> parsed_statistics_;
  std::shared_ptr<TableColumnDefinitions> partition_schema_;
  size_t current_partition_index_ = 0;
};

}  // namespace skyrise
