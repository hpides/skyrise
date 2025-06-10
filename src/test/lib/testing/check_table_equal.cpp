/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "check_table_equal.hpp"

#include <cmath>
#include <iomanip>
#include <iostream>

#include "constant_mappings.hpp"

namespace skyrise {

namespace {

constexpr double kEpsilon = 0.0001;
constexpr size_t kHeaderSize = 3;
constexpr size_t kHeaderWidth = 4;
constexpr size_t kColumnWidth = 4;

using TableCheckMatrix = std::vector<std::vector<AllTypeVariant>>;

struct TableCheckContext {
  TableCheckContext(const TableCheckMatrix& expected_matrix, const TableCheckMatrix& actual_matrix)
      : expected_matrix(expected_matrix), actual_matrix(actual_matrix) {}

  TableCheckMatrix expected_matrix;
  TableCheckMatrix actual_matrix;

  std::string error_type;
  std::string error_msg;
};

TableCheckMatrix TableToMatrix(const std::shared_ptr<const Table>& table) {
  using LeftConstIterator = boost::bimap<DataType, std::string>::left_map::const_iterator;

  // Initialize matrix with table sizes, including column names/types
  TableCheckMatrix matrix(table->RowCount() + kHeaderSize, std::vector<AllTypeVariant>(table->GetColumnCount()));

  // Set column names/types
  for (ColumnId column_id = 0; column_id < table->GetColumnCount(); ++column_id) {
    matrix[0][column_id] = std::string(table->ColumnName(column_id));

    const LeftConstIterator data_type = kDataTypeToString.left.find(table->ColumnDataType(column_id));
    if (data_type != kDataTypeToString.left.end()) {
      matrix[1][column_id] = data_type->second;
    } else {
      matrix[1][column_id] = "Invalid data type";
    }

    matrix[2][column_id] = std::string(table->ColumnIsNullable(column_id) ? "NULL" : "NOT NULL");
  }

  // set values
  ChunkOffset chunk_offset = kHeaderSize;
  ChunkOffset tmp_chunk_offset = 0;
  for (ChunkId chunk_id = 0; chunk_id < table->ChunkCount(); ++chunk_id) {
    auto chunk = table->GetChunk(chunk_id);
    for (ColumnId column_id = 0; column_id < table->GetColumnCount(); ++column_id) {
      auto segment = chunk->GetSegment(column_id);
      for (tmp_chunk_offset = 0; tmp_chunk_offset < segment->Size(); ++tmp_chunk_offset) {
        matrix[chunk_offset + tmp_chunk_offset][column_id] = (*segment)[tmp_chunk_offset];
      }
    }
    chunk_offset = chunk_offset + tmp_chunk_offset;
  }

  return matrix;
}

std::string MatrixToString(const TableCheckMatrix& matrix) {
  std::stringstream stream;

  for (size_t row_id = 0; row_id < matrix.size(); ++row_id) {
    if (row_id >= kHeaderSize) {
      stream << std::setw(kHeaderWidth) << std::to_string(row_id - kHeaderSize);
    } else {
      stream << std::setw(kHeaderWidth) << "    ";
    }

    for (const auto& column : matrix[row_id]) {
      stream << std::setw(kColumnWidth) << column << " ";
    }
    stream << "\n";
  }
  return stream.str();
}

template <typename T>
bool AlmostEquals(T a, T b, FloatComparisonMode float_comparison_mode) {
  static_assert(std::is_floating_point_v<T>, "Values must be of floating point type.");
  if (float_comparison_mode == FloatComparisonMode::kAbsoluteDifference) {
    return std::fabs(a - b) < kEpsilon;
  } else {
    return std::fabs(a - b) < std::max(kEpsilon, std::fabs(b * kEpsilon));
  }
}

void PrintTable(const TableCheckContext& context, std::stringstream* stream) {
  (*stream) << "===================== Tables are not equal ====================="
            << "\n";
  (*stream) << "------------------------- Actual Result ------------------------"
            << "\n";
  (*stream) << MatrixToString(context.actual_matrix);
  (*stream) << "----------------------------------------------------------------"
            << "\n\n";
  (*stream) << "------------------------ Expected Result -----------------------"
            << "\n";
  (*stream) << MatrixToString(context.expected_matrix);
  (*stream) << "----------------------------------------------------------------"
            << "\n";
  (*stream) << "Type of error: " << context.error_type << "\n";
  (*stream) << "================================================================"
            << "\n\n";
  (*stream) << context.error_msg << "\n\n";
}

}  // namespace

std::optional<std::string> CheckTableEqual(const std::shared_ptr<const Table>& actual_table,
                                           const std::shared_ptr<const Table>& expected_table,
                                           OrderSensitivity order_sensitivity,
                                           FloatComparisonMode float_comparison_mode) {
  if (!actual_table && expected_table) {
    return "No 'actual' table given";
  }
  if (actual_table && !expected_table) {
    return "No 'expected' table given";
  }
  if (!actual_table && !expected_table) {
    return "No 'expected' table and no 'actual' table given";
  }

  std::stringstream stream;

  auto actual_matrix = TableToMatrix(actual_table);
  auto expected_matrix = TableToMatrix(expected_table);

  // Create Context
  TableCheckContext context(expected_matrix, actual_matrix);

  // Sort if order does not matter
  if (order_sensitivity == OrderSensitivity::kNo) {
    // Skip header when sorting
    std::sort(actual_matrix.begin() + kHeaderSize, actual_matrix.end());
    std::sort(expected_matrix.begin() + kHeaderSize, expected_matrix.end());
  }

  //  Column count
  if (actual_table->GetColumnCount() != expected_table->GetColumnCount()) {
    context.error_type = "Column count mismatch";
    context.error_msg = "Actual number of columns: " + std::to_string(actual_table->GetColumnCount()) + "\n" +
                        "Expected number of columns: " + std::to_string(expected_table->GetColumnCount());

    PrintTable(context, &stream);
    return stream.str();
  }

  // Compare schema of tables
  if (!(expected_table->ColumnDefinitions() == actual_table->ColumnDefinitions())) {
    stream << "------------------------- Actual Schema ------------------------"
           << "\n";
    for (const auto& table_column_definition : actual_table->ColumnDefinitions()) {
      stream << table_column_definition << "\n";
    }
    stream << "------------------------ Expected Result -----------------------"
           << "\n";
    for (const auto& table_column_defintion : expected_table->ColumnDefinitions()) {
      stream << table_column_defintion << "\n";
    }
    return stream.str();
  }

  if (actual_table->RowCount() != expected_table->RowCount()) {
    context.error_type = "Row count mismatch";
    context.error_msg = "Actual number of rows: " + std::to_string(actual_table->RowCount()) + "\n" +
                        "Expected number of rows: " + std::to_string(expected_table->RowCount());

    PrintTable(context, &stream);
    return stream.str();
  }

  std::vector<std::pair<size_t, ColumnId>> mismatched_cells;
  // Compare each cell, skipping header
  for (size_t row_id = kHeaderSize; row_id < actual_matrix.size(); ++row_id) {
    for (ColumnId column_id = 0; column_id < actual_matrix[row_id].size(); ++column_id) {
      if (VariantIsNull(actual_matrix[row_id][column_id]) || VariantIsNull(expected_matrix[row_id][column_id])) {
        if (!(VariantIsNull(actual_matrix[row_id][column_id]) && VariantIsNull(expected_matrix[row_id][column_id]))) {
          mismatched_cells.emplace_back(row_id, column_id);
        }
      } else if (actual_table->ColumnDataType(column_id) == DataType::kFloat) {
        auto* actual_value = std::get_if<float>(&actual_matrix[row_id][column_id]);
        auto* expected_value = std::get_if<float>(&expected_matrix[row_id][column_id]);
        if (!actual_value || !expected_value ||
            !AlmostEquals(static_cast<double>(*actual_value), static_cast<double>(*expected_value),
                          float_comparison_mode)) {
          mismatched_cells.emplace_back(row_id, column_id);
        }
      } else if (actual_table->ColumnDataType(column_id) == DataType::kDouble) {
        auto* actual_value = std::get_if<double>(&actual_matrix[row_id][column_id]);
        auto* expected_value = std::get_if<double>(&expected_matrix[row_id][column_id]);
        if (!actual_value || !expected_value || !AlmostEquals(*actual_value, *expected_value, float_comparison_mode)) {
          mismatched_cells.emplace_back(row_id, column_id);
        }
      } else if (actual_matrix[row_id][column_id] != expected_matrix[row_id][column_id]) {
        mismatched_cells.emplace_back(row_id, column_id);
      }
    }
  }

  if (!mismatched_cells.empty()) {
    context.error_type = "Cell data mismatch";
    context.error_msg = "Mismatched cells (row,column): ";
    for (const auto& cell : mismatched_cells) {
      context.error_msg += "(" + std::to_string(cell.first - kHeaderSize) + "," + std::to_string(cell.second) + ") ";
    }

    PrintTable(context, &stream);
    return stream.str();
  }

  return std::nullopt;
}

}  // namespace skyrise
