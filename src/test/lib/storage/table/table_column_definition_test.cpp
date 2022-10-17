/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "storage/table/table_column_definition.hpp"

#include <gtest/gtest.h>

namespace skyrise {

class TableColumnDefinitionTest : public ::testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(TableColumnDefinitionTest, EqualityCheck) {
  const TableColumnDefinition column_definition{"a", DataType::kInt, false};
  const TableColumnDefinition equal_column_definition{"a", DataType::kInt, false};
  const TableColumnDefinition different_column_definition_a{"c", DataType::kInt, false};
  const TableColumnDefinition different_column_definition_b{"a", DataType::kDouble, false};
  const TableColumnDefinition different_column_definition_c{"a", DataType::kInt, true};

  EXPECT_EQ(column_definition, equal_column_definition);
  // `operator!=` is not implemented for TableColumnDefinition,
  // therefore EXPECT_FALSE is used instead of EXPECT_NE
  EXPECT_FALSE(column_definition == different_column_definition_a);
  EXPECT_FALSE(column_definition == different_column_definition_b);
  EXPECT_FALSE(column_definition == different_column_definition_c);
}

}  // namespace skyrise
