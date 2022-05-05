/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/stored_table_node.hpp"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "constraint_test_utils.hpp"
#include "expression/expression_functional.hpp"
#include "metadata/mock_catalog.hpp"
#include "storage/table/table_key_constraint.hpp"

// using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class StoredTableNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_catalog_ = std::make_shared<MockCatalog>();
    mock_catalog_->AddTableSchemaFromFileHeader("t_a", "resources/test_data/tbl/int_int_float.tbl");
    mock_catalog_->AddTableSchemaFromFileHeader("t_b", "resources/test_data/tbl/int_int_float.tbl");

    stored_table_node_ = StoredTableNode::Make("t_a");
    a_ = stored_table_node_->get_column("a");
    b_ = stored_table_node_->get_column("b");
    c_ = stored_table_node_->get_column("c");
  }

  std::shared_ptr<MockCatalog> mock_catalog_;
  std::shared_ptr<StoredTableNode> stored_table_node_;
  std::shared_ptr<LqpColumnExpression> a_, b_, c_;
};

TEST_F(StoredTableNodeTest, Description) {
  const auto stored_table_node_a = StoredTableNode::Make("t_a");
  EXPECT_EQ(stored_table_node_a->Description(), "[StoredTable] Name: 't_a' pruned: 0/3 column(s)");

  const auto stored_table_node_b = StoredTableNode::Make("t_a");
  stored_table_node_b->set_pruned_column_ids({ColumnId{1}});
  EXPECT_EQ(stored_table_node_b->Description(), "[StoredTable] Name: 't_a' pruned: 1/3 column(s)");
}

TEST_F(StoredTableNodeTest, GetColumn) {
  EXPECT_EQ(*stored_table_node_->get_column("a"), *a_);
  EXPECT_EQ(*stored_table_node_->get_column("b"), *b_);

  // Column pruning does not interfere with get_column()
  stored_table_node_->set_pruned_column_ids({ColumnId{0}});
  EXPECT_EQ(*stored_table_node_->get_column("a"), *a_);
  EXPECT_EQ(*stored_table_node_->get_column("b"), *b_);
}

TEST_F(StoredTableNodeTest, ColumnExpressions) {
  EXPECT_EQ(stored_table_node_->OutputExpressions().size(), 3u);
  EXPECT_EQ(*stored_table_node_->OutputExpressions().at(0u), *a_);
  EXPECT_EQ(*stored_table_node_->OutputExpressions().at(1u), *b_);
  EXPECT_EQ(*stored_table_node_->OutputExpressions().at(2u), *c_);

  // Column pruning does not interfere with get_column()
  stored_table_node_->set_pruned_column_ids({ColumnId{0}});
  EXPECT_EQ(stored_table_node_->OutputExpressions().size(), 2u);
  EXPECT_EQ(*stored_table_node_->OutputExpressions().at(0u), *b_);
  EXPECT_EQ(*stored_table_node_->OutputExpressions().at(1u), *c_);
}

TEST_F(StoredTableNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*stored_table_node_, *stored_table_node_);

  const auto different_node_a = StoredTableNode::Make("t_b");

  const auto different_node_b = StoredTableNode::Make("t_a");

  const auto different_node_c = StoredTableNode::Make("t_b");
  different_node_c->set_pruned_column_ids({ColumnId{1}});
  const auto different_node_c2 = StoredTableNode::Make("t_b");
  different_node_c2->set_pruned_column_ids({ColumnId{1}});

  EXPECT_NE(*stored_table_node_, *different_node_a);
  EXPECT_NE(*stored_table_node_, *different_node_b);
  EXPECT_NE(*stored_table_node_, *different_node_c);
  EXPECT_EQ(*different_node_c, *different_node_c2);

  EXPECT_NE(stored_table_node_->Hash(), different_node_a->Hash());
  EXPECT_NE(stored_table_node_->Hash(), different_node_b->Hash());
  EXPECT_NE(stored_table_node_->Hash(), different_node_c->Hash());
  EXPECT_EQ(different_node_c->Hash(), different_node_c2->Hash());
}

TEST_F(StoredTableNodeTest, Copy) {
  EXPECT_EQ(*stored_table_node_->DeepCopy(), *stored_table_node_);

  stored_table_node_->set_pruned_column_ids({ColumnId{1}});
  EXPECT_EQ(*stored_table_node_->DeepCopy(), *stored_table_node_);
}

TEST_F(StoredTableNodeTest, NodeExpressions) { ASSERT_EQ(stored_table_node_->node_expressions_.size(), 0u); }

TEST_F(StoredTableNodeTest, FunctionalDependenciesNone) {
  // No constraints => No functional dependencies
  EXPECT_TRUE(stored_table_node_->FunctionalDependencies().empty());

  // Constraint across all columns => No more columns available to create a functional dependency from
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");
  table_schema->AddKeyConstraint(
      {{a_->original_column_id_, b_->original_column_id_, c_->original_column_id_}, KeyConstraintType::kUnique});

  EXPECT_TRUE(stored_table_node_->FunctionalDependencies().empty());
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesSingle) {
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");
  table_schema->AddKeyConstraint({{a_->original_column_id_}, KeyConstraintType::kUnique});

  const auto& fds = stored_table_node_->FunctionalDependencies();
  const FunctionalDependency fd_expected({a_}, {b_, c_});

  EXPECT_EQ(fds.size(), 1);
  EXPECT_EQ(fds.at(0), fd_expected);
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesPrunedLeftColumnSet) {
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");
  table_schema->AddKeyConstraint({{a_->original_column_id_}, KeyConstraintType::kUnique});

  // Prune unique column "a", which would be part of the left column set in the resulting FD: {a} => {b, c}
  stored_table_node_->set_pruned_column_ids({ColumnId{0}});

  EXPECT_TRUE(stored_table_node_->FunctionalDependencies().empty());
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesPrunedLeftColumnSet2) {
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");
  table_schema->AddKeyConstraint({{b_->original_column_id_}, KeyConstraintType::kUnique});

  // Prune unique column "a", which would be part of the left column set in the resulting FD: {a} => {b, c}
  stored_table_node_->set_pruned_column_ids({ColumnId{0}});

  const FunctionalDependency fd_expected({b_}, {c_});
  EXPECT_EQ(stored_table_node_->FunctionalDependencies().size(), 1);
  EXPECT_EQ(stored_table_node_->FunctionalDependencies().at(0), fd_expected);
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesPrunedRightColumnSet) {
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");
  table_schema->AddKeyConstraint({{a_->original_column_id_}, KeyConstraintType::kUnique});

  // Prune column "b", which would be part of the right column set in the resulting FD: {a} => {b, c}
  stored_table_node_->set_pruned_column_ids({ColumnId{1}});

  const FunctionalDependency fd_expected({a_}, {c_});
  EXPECT_EQ(stored_table_node_->FunctionalDependencies().size(), 1);
  EXPECT_EQ(stored_table_node_->FunctionalDependencies().at(0), fd_expected);
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesMultiple) {
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");  // int_int_float.tbl
  table_schema->AddKeyConstraint({{a_->original_column_id_}, KeyConstraintType::kUnique});
  table_schema->AddKeyConstraint({{a_->original_column_id_, b_->original_column_id_}, KeyConstraintType::kUnique});

  const auto& fds = stored_table_node_->FunctionalDependencies();

  const FunctionalDependency fd1_expected({a_}, {b_, c_});
  const FunctionalDependency fd2_expected({a_, b_}, {c_});

  EXPECT_EQ(fds.size(), 2);
  EXPECT_EQ(fds.at(0), fd1_expected);
  EXPECT_EQ(fds.at(1), fd2_expected);
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesExcludeNullableColumns) {
  // Create four identical tables of 3 columns (a, b, c), where the second column of which is nullable (b)
  TableColumnDefinitions column_definitions{
      {"a", DataType::kInt, false}, {"b", DataType::kInt, true}, {"c", DataType::kInt, false}};

  // Test {a} => {b, c}
  {
    auto table_schema = TableSchema::FromTableColumnDefinitions(column_definitions);
    table_schema->AddKeyConstraint({{ColumnId{0}}, KeyConstraintType::kUnique});
    mock_catalog_->AddTableSchema("table_a", table_schema);

    const auto stored_table_node = StoredTableNode::Make("table_a");
    const auto& a = stored_table_node->get_column("a");
    const auto& b = stored_table_node->get_column("b");
    const auto& c = stored_table_node->get_column("c");
    const auto& fds = stored_table_node->FunctionalDependencies();

    const FunctionalDependency fd_expected({a}, {b, c});
    EXPECT_EQ(fds.size(), 1);
    EXPECT_EQ(fds.at(0), fd_expected);
  }

  // Test {a, b} => {c}
  {
    auto table_schema = TableSchema::FromTableColumnDefinitions(column_definitions);
    table_schema->AddKeyConstraint({{ColumnId{0}, ColumnId{1}}, KeyConstraintType::kUnique});
    mock_catalog_->AddTableSchema("table_b", table_schema);

    const auto& stored_table_node = StoredTableNode::Make("table_b");

    EXPECT_EQ(stored_table_node->FunctionalDependencies().size(), 0);
  }

  // Test {a, c} => {b}
  {
    auto table_schema = TableSchema::FromTableColumnDefinitions(column_definitions);
    table_schema->AddKeyConstraint({{ColumnId{0}, ColumnId{2}}, KeyConstraintType::kUnique});
    mock_catalog_->AddTableSchema("table_c", table_schema);

    const auto& stored_table_node = StoredTableNode::Make("table_c");
    const auto& a = stored_table_node->get_column("a");
    const auto& b = stored_table_node->get_column("b");
    const auto& c = stored_table_node->get_column("c");
    const auto& fds = stored_table_node->FunctionalDependencies();

    const FunctionalDependency fd_expected({a, c}, {b});
    EXPECT_EQ(fds.size(), 1);
    EXPECT_EQ(fds.at(0), fd_expected);
  }

  // Test {b} => {a, c}
  {
    auto table_schema = TableSchema::FromTableColumnDefinitions(column_definitions);
    table_schema->AddKeyConstraint({{ColumnId{1}}, KeyConstraintType::kUnique});
    mock_catalog_->AddTableSchema("table_d", table_schema);

    const auto& stored_table_node = StoredTableNode::Make("table_d");

    EXPECT_EQ(stored_table_node->FunctionalDependencies().size(), 0);
  }
}

TEST_F(StoredTableNodeTest, UniqueConstraints) {
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");

  const TableKeyConstraint key_constraint_a_b({ColumnId{0}, ColumnId{1}}, KeyConstraintType::kPrimaryKey);
  const TableKeyConstraint key_constraint_c({ColumnId{2}}, KeyConstraintType::kUnique);
  table_schema->AddKeyConstraint(key_constraint_a_b);
  table_schema->AddKeyConstraint(key_constraint_c);

  const auto& unique_constraints = stored_table_node_->UniqueConstraints();

  // Basic check
  EXPECT_EQ(unique_constraints->size(), 2);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_a_b, unique_constraints));
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_c, unique_constraints));

  // Check whether StoredTableNode is referenced by the constraint's expressions
  for (const auto& unique_constraint : *unique_constraints) {
    for (const auto& expression : unique_constraint.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LqpColumnExpression>(expression);
      EXPECT_TRUE(column_expression && !column_expression->original_node_.expired());
      EXPECT_TRUE(column_expression->original_node_.lock() == stored_table_node_);
    }
  }
}

TEST_F(StoredTableNodeTest, UniqueConstraintsPrunedColumns) {
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");

  // Prepare unique constraints
  const TableKeyConstraint key_constraint_a({ColumnId{0}}, KeyConstraintType::kUnique);
  const TableKeyConstraint key_constraint_a_b({ColumnId{0}, ColumnId{1}}, KeyConstraintType::kUnique);
  const TableKeyConstraint key_constraint_c({ColumnId{2}}, KeyConstraintType::kUnique);
  table_schema->AddKeyConstraint(key_constraint_a);
  table_schema->AddKeyConstraint(key_constraint_a_b);
  table_schema->AddKeyConstraint(key_constraint_c);

  const auto& table_key_constraints = table_schema->KeyConstraints();
  EXPECT_EQ(table_key_constraints.size(), 3);
  EXPECT_EQ(stored_table_node_->UniqueConstraints()->size(), 3);

  // Prune column a, which should remove two unique constraints
  stored_table_node_->set_pruned_column_ids({ColumnId{0}});

  // Basic check
  const auto& unique_constraints = stored_table_node_->UniqueConstraints();
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_c, unique_constraints));
}

TEST_F(StoredTableNodeTest, UniqueConstraintsEmpty) {
  ASSERT_TRUE(mock_catalog_->GetEditableTableSchema(stored_table_node_->table_name)->KeyConstraints().empty());
  EXPECT_TRUE(stored_table_node_->UniqueConstraints()->empty());
}

TEST_F(StoredTableNodeTest, HasMatchingUniqueConstraint) {
  auto table_schema = mock_catalog_->GetEditableTableSchema("t_a");
  const TableKeyConstraint key_constraint_a({a_->original_column_id_}, KeyConstraintType::kUnique);
  table_schema->AddKeyConstraint(key_constraint_a);
  EXPECT_EQ(stored_table_node_->UniqueConstraints()->size(), 1);

  // Negative test
  EXPECT_FALSE(stored_table_node_->HasMatchingUniqueConstraint({b_}));
  EXPECT_FALSE(stored_table_node_->HasMatchingUniqueConstraint({c_}));
  EXPECT_FALSE(stored_table_node_->HasMatchingUniqueConstraint({b_, c_}));

  // Test exact match
  EXPECT_TRUE(stored_table_node_->HasMatchingUniqueConstraint({a_}));

  // Test superset of column ids
  EXPECT_TRUE(stored_table_node_->HasMatchingUniqueConstraint({a_, b_}));
  EXPECT_TRUE(stored_table_node_->HasMatchingUniqueConstraint({a_, c_}));
}

}  // namespace skyrise
