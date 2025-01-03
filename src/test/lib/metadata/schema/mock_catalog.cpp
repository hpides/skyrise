#include "mock_catalog.hpp"

#include <fstream>

#include "constant_mappings.hpp"
#include "metadata/schema/table_schema.hpp"
#include "storage/table/table_column_definition.hpp"
#include "utils/string.hpp"

namespace {
const std::string kMockBucketName = "mock-table-bucket";
const std::string kMockPartitionEtag = "mock-etag";
const size_t kMockPartitionCount = 3;
const size_t kMockPartitionSize = 0;
const time_t kMockPartitionTimestamp = 0;
}  // namespace

namespace skyrise {

void MockCatalog::AddTableSchema(const std::string& table_name, const std::shared_ptr<TableSchema>& table_schema) {
  const bool inserted = table_name_to_table_schema_.try_emplace(table_name, table_schema).second;
  Assert(inserted, "Cannot add TableSchema for table name '" + table_name + "' because it already exists.");

  // Generate mock TablePartitions
  std::vector<TablePartition> table_partitions;
  table_partitions.reserve(kMockPartitionCount);
  for (size_t i = 1; i <= kMockPartitionCount; ++i) {
    const std::string object_key = table_name + "_object0" + std::to_string(i) + GetFormatExtension(FileFormat::kOrc);
    table_partitions.emplace_back(object_key, kMockPartitionEtag, kMockPartitionSize, kMockPartitionTimestamp);
  }
  table_name_to_table_partitions_.emplace(table_name, std::move(table_partitions));
}

void MockCatalog::AddTableSchemaFromFileHeader(const std::string& table_name, const std::string& file_name) {
  std::ifstream infile(file_name);
  Assert(infile.is_open(), "Could not find file " + file_name);

  std::string line;
  std::getline(infile, line);
  Assert(line.find('\r') == std::string::npos, "Windows encoding is not supported, use dos2unix.");
  std::vector<std::string> column_names = SplitStringByDelimiter(line, '|');
  std::getline(infile, line);
  std::vector<std::string> column_types = SplitStringByDelimiter(line, '|');
  std::vector<bool> column_nullable(column_types.size());

  for (auto& column_type : column_types) {
    // type can be, for example, 'int' or 'int_null'
    const std::vector<std::string> type_components = SplitStringByDelimiter(column_type, '_');

    bool is_nullable = false;
    if (type_components.size() > 1) {
      Assert(type_components[1] == "null" && type_components.size() <= 2, "Invalid type specification.");
      is_nullable = true;
    }
    column_nullable.push_back(is_nullable);

    // Remove `_null` suffix from the column_type string to match a DataType in the next code block
    column_type = type_components[0];
  }

  TableColumnDefinitions column_definitions;
  for (size_t i = 0; i < column_names.size(); ++i) {
    const auto data_type = kDataTypeToString.right.find(column_types[i]);
    Assert(data_type != kDataTypeToString.right.end(),
           std::string("Invalid data type ") + column_types[i] + " for column " + column_names[i]);
    column_definitions.emplace_back(column_names[i], data_type->second, column_nullable[i]);
  }

  AddTableSchema(table_name, TableSchema::FromTableColumnDefinitions(column_definitions));
}

bool MockCatalog::TableExists(const std::string& table_name) const {
  return table_name_to_table_schema_.find(table_name) != table_name_to_table_schema_.cend();
}

std::shared_ptr<TableSchema> MockCatalog::EditableTableSchema(const std::string& table_name) {
  auto table_name_to_table_schema_iterator = table_name_to_table_schema_.find(table_name);
  Assert(table_name_to_table_schema_iterator != table_name_to_table_schema_.cend(),
         "Could not find TableSchema for table '" + table_name + "'.");
  return table_name_to_table_schema_iterator->second;
}

std::shared_ptr<const TableSchema> MockCatalog::GetTableSchema(const std::string& table_name) const {
  auto table_name_to_table_schema_iterator = table_name_to_table_schema_.find(table_name);
  Assert(table_name_to_table_schema_iterator != table_name_to_table_schema_.cend(),
         "Could not find TableSchema for table '" + table_name + "'.");
  return std::const_pointer_cast<const TableSchema>(table_name_to_table_schema_iterator->second);
}

const std::string& MockCatalog::TableBucketName(const std::string& /*table_name*/) const { return kMockBucketName; }

const std::vector<TablePartition>& MockCatalog::GetTablePartitions(const std::string& table_name) const {
  Assert(TableExists(table_name), "Table does not exist.");
  return table_name_to_table_partitions_.find(table_name)->second;
}

}  // namespace skyrise
