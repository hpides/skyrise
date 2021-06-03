#include "statistics_collector.hpp"

namespace skyrise {

ObjectStatistics StatisticsOrcFormatReader::GetAllStatistics() const {
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

size_t StatisticsOrcFormatReader::GetNumColumns() const {
  const auto& type = reader_->getType();
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

size_t StatisticsOrcFormatReader::GetNullCountForColumn(size_t column_index) const {
  const orc::Type* orc_type = reader_->getType().getSubtype(column_index);
  std::unique_ptr<orc::ColumnStatistics> stats = reader_->getColumnStatistics(orc_type->getColumnId());
  return GetNumRows() - stats->getNumberOfValues();
}

std::pair<AllTypeVariant, AllTypeVariant> StatisticsOrcFormatReader::ConvertDate(
    std::pair<AllTypeVariant, AllTypeVariant> minmax) {
  if (!std::holds_alternative<NullValue>(minmax.first)) {
    minmax.first = OrcTimestampToDateString(std::get<int32_t>(minmax.first));
  }
  if (!std::holds_alternative<NullValue>(minmax.second)) {
    minmax.second = OrcTimestampToDateString(std::get<int32_t>(minmax.second));
  }

  return minmax;
}

std::pair<AllTypeVariant, AllTypeVariant> StatisticsOrcFormatReader::GetMinMaxForColumn(size_t column_index) const {
  const orc::Type* orc_type = reader_->getType().getSubtype(column_index);
  std::unique_ptr<orc::ColumnStatistics> stats = reader_->getColumnStatistics(orc_type->getColumnId());

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

size_t StatisticsOrcFormatReader::GetNumRows() const { return reader_->getNumberOfRows(); }

std::shared_ptr<TableColumnDefinitions> StatisticsOrcFormatReader::GetSchema() const {
  auto schema = std::make_shared<TableColumnDefinitions>();

  const auto& type = reader_->getType();
  for (size_t i = 0; i < type.getSubtypeCount(); i++) {
    const orc::Type* orc_type = type.getSubtype(i);
    DataType skyrise_type = OrcTypeKindToDataType(orc_type->getKind());

    // The current ORC definition has no information about whether or not NULL values are allowed for a column.
    bool nullable = false;

    schema->emplace_back(type.getFieldName(i), skyrise_type, nullable);
  }

  return schema;
}

}  // namespace skyrise
