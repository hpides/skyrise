#pragma once

#include <chrono>
#include <memory>
#include <orc/OrcFile.hh>

#include <aws/core/utils/logging/LogMacros.h>

#include "storage/backend/abstract_storage.hpp"
#include "storage/table/table_column_definition.hpp"
#include "utils/literal.hpp"

namespace skyrise {

namespace detail {

DataType ConvertOrcTypeToSkyriseType(orc::TypeKind type);
std::string GetDateFromOrcTimestamp(int32_t days_since_1970);

class OrcInputStream : public orc::InputStream {
 public:
  OrcInputStream(const std::shared_ptr<Storage>& storage, const std::string& object_identifier);
  OrcInputStream(const std::shared_ptr<Storage>& storage, const ObjectStatus& status);
  uint64_t getLength() const override { return file_size_; }
  uint64_t getNaturalReadSize() const override { return 20_MB; }
  void read(void* buf, uint64_t length, uint64_t offset) override;
  const std::string& getName() const override { return object_identifier_; }

 private:
  void InitWithStatus(const std::shared_ptr<Storage>& storage, const ObjectStatus& status);

  std::unique_ptr<ObjectReader> reader_;
  std::string object_identifier_;
  size_t file_size_ = 0;
  bool error_ = false;
};
}  // namespace detail

struct ObjectStatistics {
  // Information about the object itself.
  std::string object_identifier;
  std::string format;
  std::string etag;
  time_t last_modified;
  size_t filesize;
  size_t num_rows = 0;

  // Information about the content of the object.
  std::vector<size_t> null_count;
  std::shared_ptr<TableColumnDefinitions> schema;

  // MinMax statistics.
  std::vector<std::pair<AllTypeVariant, AllTypeVariant>> minmax;
};

class StatisticsCollector {
 public:
  static constexpr auto kFormatOrc = "orc";

  StatisticsCollector(std::shared_ptr<Storage> storage, const ObjectStatus& object)
      : storage_(std::move(storage)), object_status_(object) {
    InitReader(object_status_);
  }

  size_t GetNumColumns();
  size_t GetNumRows();
  std::shared_ptr<TableColumnDefinitions> GetSchema();

  std::pair<AllTypeVariant, AllTypeVariant> GetMinMaxForColumn(size_t column_index);
  size_t GetNullCountForColumn(size_t column_index);
  ObjectStatistics GetAllStatistics();

 private:
  void InitReader(const ObjectStatus& object);
  std::shared_ptr<Storage> storage_;
  ObjectStatus object_status_;
  std::unique_ptr<orc::Reader> orc_reader_;
};

}  // namespace skyrise
