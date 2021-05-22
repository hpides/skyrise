#pragma once

#include <optional>

#include "serialization/schema_serialization.hpp"
#include "statistics/statistics_collector.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/orc_writer.hpp"

namespace skyrise {

class ManifestWriter {
 public:
  ManifestWriter(std::unique_ptr<ObjectWriter> writer);

  static std::string GetManifestVersion();
  bool WritePartition(const ObjectStatistics& statistics);
  void SetSchema(std::shared_ptr<TableColumnDefinitions> partition_schema);
  void SetTablePrefix(std::string table_prefix);
  bool Close();
  StorageError GetError();

 private:
  void AssertOutputStream();
  bool WritePartitionToStorage(const ObjectStatistics& statistics);
  TableColumnDefinitions GetManifestSchema();
  static std::shared_ptr<BaseValueSegment> CreateSegmentForDataType(DataType type, bool nullable = false);
  void InitManifestSegments();
  void Flush();

  static constexpr size_t kMaxCapacity = 1024;
  static constexpr int kManifestVersion = 1;

  std::unique_ptr<ObjectWriter> writer_;
  std::unique_ptr<OrcFormatter> formatter_;
  std::shared_ptr<TableColumnDefinitions> partition_schema_;
  std::string table_prefix_;
  std::vector<std::shared_ptr<BaseValueSegment>> current_segments_;
  StorageError error_;
};

}  // namespace skyrise
