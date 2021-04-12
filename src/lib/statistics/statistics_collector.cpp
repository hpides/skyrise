#include "statistics_collector.hpp"

#include <ctime>
#include <sstream>

namespace skyrise {

namespace detail {

std::string GetDateFromOrcTimestamp(int32_t days_since_1970) {
  time_t seconds_since_1970 = static_cast<time_t>(days_since_1970) * (60 * 60 * 24);
  tm calendar_date{};
  std::array<char, 11> buffer = {0};  // YYYY-mm-dd + '\0'
  localtime_r(&seconds_since_1970, &calendar_date);
  strftime(buffer.data(), 11, "%Y-%m-%d", &calendar_date);
  return std::string(buffer.data());
}

DataType ConvertOrcTypeToSkyriseType(orc::TypeKind type) {
  switch (type) {
    case orc::FLOAT:
      return DataType::kFloat;

    case orc::DOUBLE:
    case orc::DECIMAL:
      return DataType::kDouble;

    case orc::INT:
    case orc::BYTE:
    case orc::BOOLEAN:
    case orc::SHORT:
      return DataType::kInt;

    case orc::LONG:
    case orc::TIMESTAMP:
      return DataType::kLong;

    case orc::STRING:
    case orc::CHAR:
    case orc::VARCHAR:
    case orc::BINARY:
    case orc::DATE:
      return DataType::kString;

    default:
      std::stringstream fail_message;
      fail_message << "Encountered unsupported type: " << magic_enum::enum_name(type);
      Fail(fail_message.str());
  }
}

OrcInputStream::OrcInputStream(const std::shared_ptr<Storage>& storage, const std::string& object_identifier) {
  ObjectStatus status = storage->GetStatus(object_identifier);
  if (status.GetError()) {
    error_ = true;
    throw orc::ParseError("Could not stat file.");
    return;
  }
  InitWithStatus(storage, status);
}

void OrcInputStream::InitWithStatus(const std::shared_ptr<Storage>& storage, const ObjectStatus& status) {
  file_size_ = status.GetSize();
  reader_ = storage->OpenForReading(status.GetIdentifier());
  object_identifier_ = status.GetIdentifier();
}

OrcInputStream::OrcInputStream(const std::shared_ptr<Storage>& storage, const ObjectStatus& status) {
  InitWithStatus(storage, status);
}

void OrcInputStream::read(void* buffer, uint64_t length, uint64_t offset) {
  if (error_) {
    throw orc::ParseError("Could not read from file.");
  }

  size_t read_so_far = 0;
  StorageError error = reader_->Read(offset, offset + length - 1, [&](const char* data, size_t length) {
    memcpy(&static_cast<char*>(buffer)[read_so_far], data, length);
    read_so_far += length;
  });

  if (error) {
    error_ = true;
    throw orc::ParseError("Could not read from file.");
  }
}

}  // namespace detail

void StatisticsCollector::InitReader(const ObjectStatus& object) {
  auto orc_input = std::make_unique<detail::OrcInputStream>(storage_, object);
  orc::ReaderOptions options;
  orc_reader_ = orc::createReader(std::move(orc_input), options);
}

ObjectStatistics StatisticsCollector::GetAllStatistics() {
  size_t num_rows = GetNumColumns();
  std::vector<size_t> null_count;
  std::vector<std::pair<AllTypeVariant, AllTypeVariant>> minmax;

  for (size_t i = 0; i < num_rows; i++) {
    null_count.push_back(GetNullCountForColumn(i));
    minmax.push_back(GetMinMaxForColumn(i));
  }
  return ObjectStatistics{object_status_.GetIdentifier(),
                          kFormatOrc,  // We only support ORC so far.
                          object_status_.GetChecksum(),
                          object_status_.GetLastModifiedTimestamp(),
                          object_status_.GetSize(),
                          GetNumRows(),
                          null_count,
                          GetSchema(),
                          minmax};
}

size_t StatisticsCollector::GetNumColumns() {
  const auto& type = orc_reader_->getType();
  if (type.getKind() == orc::STRUCT) {
    return type.getSubtypeCount();
  } else {
    return -1;
  }
}

template <class T>
std::pair<AllTypeVariant, AllTypeVariant> GetMinMax(const std::unique_ptr<orc::ColumnStatistics>& statistics) {
  AllTypeVariant minimum{};
  AllTypeVariant maximum{};
  T* casted_stats = dynamic_cast<T*>(statistics.get());
  if (casted_stats != nullptr) {
    if (casted_stats->hasMinimum()) {
      minimum = casted_stats->getMinimum();
    }
    if (casted_stats->hasMaximum()) {
      maximum = casted_stats->getMaximum();
    }
  }
  return std::make_pair(minimum, maximum);
}
template <class T, class I>
std::pair<AllTypeVariant, AllTypeVariant> GetMinMaxNumeric(const std::unique_ptr<orc::ColumnStatistics>& statistics) {
  AllTypeVariant minimum{};
  AllTypeVariant maximum{};
  T* casted_stats = dynamic_cast<T*>(statistics.get());
  if (casted_stats != nullptr) {
    if (casted_stats->hasMinimum()) {
      minimum = static_cast<I>(casted_stats->getMinimum());
    }
    if (casted_stats->hasMaximum()) {
      maximum = static_cast<I>(casted_stats->getMaximum());
    }
  }
  return std::make_pair(minimum, maximum);
}

size_t StatisticsCollector::GetNullCountForColumn(size_t column_index) {
  const orc::Type* orc_type = orc_reader_->getType().getSubtype(column_index);
  std::unique_ptr<orc::ColumnStatistics> stats = orc_reader_->getColumnStatistics(orc_type->getColumnId());
  return GetNumRows() - stats->getNumberOfValues();
}

std::pair<AllTypeVariant, AllTypeVariant> ConvertDate(std::pair<AllTypeVariant, AllTypeVariant> minmax) {
  if (!std::holds_alternative<NullValue>(minmax.first)) {
    minmax.first = detail::GetDateFromOrcTimestamp(std::get<int32_t>(minmax.first));
  }
  if (!std::holds_alternative<NullValue>(minmax.second)) {
    minmax.second = detail::GetDateFromOrcTimestamp(std::get<int32_t>(minmax.second));
  }

  return minmax;
}

std::pair<AllTypeVariant, AllTypeVariant> StatisticsCollector::GetMinMaxForColumn(size_t column_index) {
  const orc::Type* orc_type = orc_reader_->getType().getSubtype(column_index);
  std::unique_ptr<orc::ColumnStatistics> stats = orc_reader_->getColumnStatistics(orc_type->getColumnId());

  switch (orc_type->getKind()) {
    case orc::INT:
    case orc::SHORT:
      return GetMinMaxNumeric<orc::IntegerColumnStatistics, int32_t>(stats);
    case orc::LONG:
      return GetMinMaxNumeric<orc::IntegerColumnStatistics, int64_t>(stats);
    case orc::FLOAT:
      return GetMinMaxNumeric<orc::DoubleColumnStatistics, float>(stats);
    case orc::DOUBLE:
      return GetMinMaxNumeric<orc::DoubleColumnStatistics, double>(stats);
    case orc::DATE:
      return ConvertDate(GetMinMax<orc::DateColumnStatistics>(stats));
    case orc::VARCHAR:
    case orc::CHAR:
    case orc::STRING:
      return GetMinMax<orc::StringColumnStatistics>(stats);

    // TODO(anyone): Implement and test other types.
    // case orc::BYTE:
    // BOOLEAN:
    //   return GetMinMax<orc::BooleanColumnStatistics>(stats);
    // BINARY:
    //   return GetMinMax<orc::BinaryColumnStatistics>(stats);
    // TIMESTAMP:
    // return GetMinMax<orc::TimestampColumnStatistics>(stats);
    // DECIMAL:
    //   return GetMinMax<orc::DecimalColumnStatistics>(stats);
    // LIST:
    // MAP:
    // STRUCT:
    // UNION:
    default:
      return std::pair<AllTypeVariant, AllTypeVariant>{};
  }
}

size_t StatisticsCollector::GetNumRows() { return orc_reader_->getNumberOfRows(); }

std::shared_ptr<TableColumnDefinitions> StatisticsCollector::GetSchema() {
  auto schema = std::make_shared<TableColumnDefinitions>();

  const auto& type = orc_reader_->getType();
  for (size_t i = 0; i < type.getSubtypeCount(); i++) {
    const orc::Type* orc_type = type.getSubtype(i);
    DataType skyrise_type = detail::ConvertOrcTypeToSkyriseType(orc_type->getKind());

    // The current ORC definition has no information about whether or not NULL values are allowed for a column.
    bool nullable = false;

    schema->emplace_back(type.getFieldName(i), skyrise_type, nullable);
  }

  return schema;
}

}  // namespace skyrise
