#pragma once

#include "storage/formats/orc_reader.hpp"

namespace skyrise {

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
  std::shared_ptr<const TableColumnDefinitions> schema;

  // MinMax statistics.
  std::vector<std::pair<AllTypeVariant, AllTypeVariant>> minmax;
};
class StatisticsOrcFormatReader : public OrcFormatReader {
 public:
  inline static const std::string kFormatOrc{"orc"};

  StatisticsOrcFormatReader(std::shared_ptr<Storage> storage, const ObjectStatus& object)
      : OrcFormatReader(storage->OpenForReading(object.GetIdentifier())), object_status_(object) {}

  size_t GetNumColumns() const;
  size_t GetNumRows() const;
  std::shared_ptr<TableColumnDefinitions> GetSchema() const;

  std::pair<AllTypeVariant, AllTypeVariant> GetMinMaxForColumn(size_t column_index) const;
  size_t GetNullCountForColumn(size_t column_index) const;
  ObjectStatistics GetAllStatistics() const;

 private:
  ObjectStatus object_status_;
  static std::pair<AllTypeVariant, AllTypeVariant> ConvertDate(std::pair<AllTypeVariant, AllTypeVariant> minmax);
};

}  // namespace skyrise
