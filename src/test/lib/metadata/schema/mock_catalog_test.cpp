#include "metadata/schema/mock_catalog.hpp"

#include <gtest/gtest.h>

#include "storage/table/table_column_definition.hpp"

namespace skyrise {

class MockCatalogTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_catalog_ = MockCatalog();

    column_definition_a_ = TableColumnDefinition{"a", DataType::kInt, true};
    column_definition_x_ = TableColumnDefinition{"x", DataType::kFloat, false};
    column_definition_y_ = TableColumnDefinition{"y", DataType::kString, false};
  }

 protected:
  MockCatalog mock_catalog_;
  TableColumnDefinition column_definition_a_;
  TableColumnDefinition column_definition_x_;
  TableColumnDefinition column_definition_y_;
};

TEST_F(MockCatalogTest, EmptyTableSchema) {
  // Creating / adding TableSchema without column definitions should fail.
  const TableColumnDefinitions column_definitions;
  EXPECT_THROW(mock_catalog_.AddTableSchema("table_without_columns",
                                            TableSchema::FromTableColumnDefinitions(column_definitions)),
               std::logic_error);
}

TEST_F(MockCatalogTest, AddTableSchemaWithSingleColumn) {
  const TableColumnDefinitions column_definitions = {column_definition_a_};
  mock_catalog_.AddTableSchema("table_a", TableSchema::FromTableColumnDefinitions(column_definitions));

  ASSERT_TRUE(mock_catalog_.TableExists("table_a"));
  auto table_a_schema = mock_catalog_.GetTableSchema("table_a");

  EXPECT_EQ(table_a_schema->TableColumnCount(), 1);
  EXPECT_EQ(table_a_schema->ColumnName(ColumnId(0)), "a");
  EXPECT_EQ(table_a_schema->ColumnIdByName("a"), ColumnId(0));
  EXPECT_EQ(table_a_schema->ColumnDataType(ColumnId(0)), DataType::kInt);
  EXPECT_EQ(table_a_schema->ColumnIsNullable(ColumnId(0)), true);

  EXPECT_THROW(table_a_schema->ColumnName(ColumnId(1)), std::logic_error);
  EXPECT_THROW(table_a_schema->ColumnIdByName("b"), std::logic_error);
  EXPECT_THROW(table_a_schema->ColumnDataType(ColumnId(1)), std::logic_error);
  EXPECT_THROW(table_a_schema->ColumnIsNullable(ColumnId(1)), std::logic_error);
}

TEST_F(MockCatalogTest, DoNotOverrideTableSchema) {
  const TableColumnDefinitions column_definitions = {column_definition_a_};
  mock_catalog_.AddTableSchema("table", TableSchema::FromTableColumnDefinitions(column_definitions));
  // Overriding TableSchema is forbidden.
  EXPECT_THROW(mock_catalog_.AddTableSchema("table", TableSchema::FromTableColumnDefinitions(column_definitions)),
               std::logic_error);
}

TEST_F(MockCatalogTest, AddTableSchemaWithTwoColumns) {
  const TableColumnDefinitions column_definitions = {column_definition_x_, column_definition_y_};
  mock_catalog_.AddTableSchema("table_xy", TableSchema::FromTableColumnDefinitions(column_definitions));

  ASSERT_TRUE(mock_catalog_.TableExists("table_xy"));
  auto table_xy_schema = mock_catalog_.GetTableSchema("table_xy");

  EXPECT_EQ(table_xy_schema->TableColumnCount(), 2);

  EXPECT_EQ(table_xy_schema->ColumnName(ColumnId(0)), "x");
  EXPECT_EQ(table_xy_schema->ColumnIdByName("x"), ColumnId(0));
  EXPECT_EQ(table_xy_schema->ColumnDataType(ColumnId(0)), DataType::kFloat);
  EXPECT_EQ(table_xy_schema->ColumnIsNullable(ColumnId(0)), false);

  EXPECT_EQ(table_xy_schema->ColumnName(ColumnId(1)), "y");
  EXPECT_EQ(table_xy_schema->ColumnIdByName("y"), ColumnId(1));
  EXPECT_EQ(table_xy_schema->ColumnDataType(ColumnId(1)), DataType::kString);
  EXPECT_EQ(table_xy_schema->ColumnIsNullable(ColumnId(1)), false);
}

TEST_F(MockCatalogTest, AddTableSchemaFromFileHeader) {
  mock_catalog_.AddTableSchemaFromFileHeader("int_string", "resources/test_data/tbl/int_string.tbl");
  mock_catalog_.AddTableSchemaFromFileHeader("int_int_int", "resources/test_data/tbl/int_int_int.tbl");

  {
    ASSERT_TRUE(mock_catalog_.TableExists("int_string"));
    auto int_string_schema = mock_catalog_.GetTableSchema("int_string");

    EXPECT_EQ(int_string_schema->TableColumnCount(), 2);

    EXPECT_EQ(int_string_schema->ColumnName(ColumnId(0)), "a");
    EXPECT_EQ(int_string_schema->ColumnIdByName("a"), ColumnId(0));
    EXPECT_EQ(int_string_schema->ColumnDataType(ColumnId(0)), DataType::kInt);
    EXPECT_EQ(int_string_schema->ColumnIsNullable(ColumnId(0)), false);

    EXPECT_EQ(int_string_schema->ColumnName(ColumnId(1)), "b");
    EXPECT_EQ(int_string_schema->ColumnIdByName("b"), ColumnId(1));
    EXPECT_EQ(int_string_schema->ColumnDataType(ColumnId(1)), DataType::kString);
    EXPECT_EQ(int_string_schema->ColumnIsNullable(ColumnId(1)), false);
  }

  {
    ASSERT_TRUE(mock_catalog_.TableExists("int_int_int"));
    auto int_int_int_schema = mock_catalog_.GetTableSchema("int_int_int");

    EXPECT_EQ(int_int_int_schema->TableColumnCount(), 3);

    EXPECT_EQ(int_int_int_schema->ColumnName(ColumnId(0)), "a");
    EXPECT_EQ(int_int_int_schema->ColumnIdByName("a"), ColumnId(0));
    EXPECT_EQ(int_int_int_schema->ColumnDataType(ColumnId(0)), DataType::kInt);
    EXPECT_EQ(int_int_int_schema->ColumnIsNullable(ColumnId(0)), false);

    EXPECT_EQ(int_int_int_schema->ColumnName(ColumnId(1)), "b");
    EXPECT_EQ(int_int_int_schema->ColumnIdByName("b"), ColumnId(1));
    EXPECT_EQ(int_int_int_schema->ColumnDataType(ColumnId(1)), DataType::kInt);
    EXPECT_EQ(int_int_int_schema->ColumnIsNullable(ColumnId(1)), false);

    EXPECT_EQ(int_int_int_schema->ColumnName(ColumnId(2)), "c");
    EXPECT_EQ(int_int_int_schema->ColumnIdByName("c"), ColumnId(2));
    EXPECT_EQ(int_int_int_schema->ColumnDataType(ColumnId(2)), DataType::kInt);
    EXPECT_EQ(int_int_int_schema->ColumnIsNullable(ColumnId(2)), false);
  }
}

TEST_F(MockCatalogTest, TableBucketName) { EXPECT_EQ(mock_catalog_.TableBucketName("table_xy"), "mock-table-bucket"); }

TEST_F(MockCatalogTest, GetTablePartitionKeys) {
  const std::string table_name = "table_a";
  const TableColumnDefinitions column_definitions = {column_definition_a_};
  mock_catalog_.AddTableSchema(table_name, TableSchema::FromTableColumnDefinitions(column_definitions));

  EXPECT_EQ(mock_catalog_.GetTablePartitions(table_name).size(), 3);
  EXPECT_EQ(mock_catalog_.GetTablePartitions(table_name).at(0).ObjectKey(), "table_a_object01.orc");
  EXPECT_EQ(mock_catalog_.GetTablePartitions(table_name).at(1).ObjectKey(), "table_a_object02.orc");
  EXPECT_EQ(mock_catalog_.GetTablePartitions(table_name).at(2).ObjectKey(), "table_a_object03.orc");
}

}  // namespace skyrise
