/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "storage/table/table_key_constraint.hpp"

#include <memory>

#include <gtest/gtest.h>

namespace skyrise {

TEST(TableKeyConstraintTest, DuplicateColumnIds) {
  // The implementation should avoid duplicate ColumnIds.
  const TableKeyConstraint key_constraint = {{ColumnId{1}, ColumnId{1}}, KeyConstraintType::kUnique};
  EXPECT_EQ(key_constraint.ColumnIds().size(), 1);
  EXPECT_EQ(*key_constraint.ColumnIds().begin(), ColumnId{1});
}

TEST(TableKeyConstraintTest, Equals) {
  const TableKeyConstraint key_constraint_a = {{ColumnId{0}, ColumnId{2}}, KeyConstraintType::kUnique};
  const TableKeyConstraint key_constraint_a_reordered = {{ColumnId{2}, ColumnId{0}}, KeyConstraintType::kUnique};
  const TableKeyConstraint primary_key_constraint_a = {{ColumnId{0}, ColumnId{2}}, KeyConstraintType::kPrimaryKey};

  const TableKeyConstraint key_constraint_b = {{ColumnId{2}, ColumnId{3}}, KeyConstraintType::kUnique};
  const TableKeyConstraint key_constraint_c = {{ColumnId{0}}, KeyConstraintType::kUnique};

  EXPECT_TRUE(key_constraint_a == key_constraint_a);
  EXPECT_TRUE(key_constraint_a == key_constraint_a_reordered);
  EXPECT_TRUE(key_constraint_a_reordered == key_constraint_a);

  EXPECT_FALSE(key_constraint_a == primary_key_constraint_a);
  EXPECT_FALSE(primary_key_constraint_a == key_constraint_a);

  EXPECT_FALSE(key_constraint_a == key_constraint_b);
  EXPECT_FALSE(key_constraint_b == key_constraint_a);

  EXPECT_FALSE(key_constraint_c == key_constraint_a);
  EXPECT_FALSE(key_constraint_a == key_constraint_c);
}

}  // namespace skyrise
