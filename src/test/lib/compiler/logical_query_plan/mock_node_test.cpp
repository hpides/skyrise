/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/mock_node.hpp"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "constraint_test_utils.hpp"

namespace skyrise {

class MockNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_node_a_ = MockNode::Make(MockNode::ColumnDefinitions{
        {DataType::kInt, "a"}, {DataType::kFloat, "b"}, {DataType::kDouble, "c"}, {DataType::kString, "d"}});
    mock_node_b_ =
        MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kFloat, "b"}}, "mock_name");
  }

  std::shared_ptr<MockNode> mock_node_a_;
  std::shared_ptr<MockNode> mock_node_b_;
};

TEST_F(MockNodeTest, Description) {
  EXPECT_EQ(mock_node_a_->Description(), "[MockNode 'Unnamed'] Columns: a b c d | pruned: 0/4 columns");
  EXPECT_EQ(mock_node_b_->Description(), "[MockNode 'mock_name'] Columns: a b | pruned: 0/2 columns");

  mock_node_a_->SetPrunedColumnIds({ColumnId{2}});
  EXPECT_EQ(mock_node_a_->Description(), "[MockNode 'Unnamed'] Columns: a b d | pruned: 1/4 columns");
}

TEST_F(MockNodeTest, OutputColumnExpression) {
  ASSERT_EQ(mock_node_a_->OutputExpressions().size(), 4u);
  EXPECT_EQ(*mock_node_a_->OutputExpressions().at(0),
            *std::make_shared<LqpColumnExpression>(mock_node_a_, ColumnId{0}));
  EXPECT_EQ(*mock_node_a_->OutputExpressions().at(1),
            *std::make_shared<LqpColumnExpression>(mock_node_a_, ColumnId{1}));
  EXPECT_EQ(*mock_node_a_->OutputExpressions().at(2),
            *std::make_shared<LqpColumnExpression>(mock_node_a_, ColumnId{2}));
  EXPECT_EQ(*mock_node_a_->OutputExpressions().at(3),
            *std::make_shared<LqpColumnExpression>(mock_node_a_, ColumnId{3}));

  ASSERT_EQ(mock_node_b_->OutputExpressions().size(), 2u);
  EXPECT_EQ(*mock_node_b_->OutputExpressions().at(0),
            *std::make_shared<LqpColumnExpression>(mock_node_b_, ColumnId{0}));
  EXPECT_EQ(*mock_node_b_->OutputExpressions().at(1),
            *std::make_shared<LqpColumnExpression>(mock_node_b_, ColumnId{1}));

  mock_node_a_->SetPrunedColumnIds({ColumnId{0}, ColumnId{3}});
  EXPECT_EQ(mock_node_a_->OutputExpressions().size(), 2u);
  EXPECT_EQ(*mock_node_a_->OutputExpressions().at(0),
            *std::make_shared<LqpColumnExpression>(mock_node_a_, ColumnId{1}));
  EXPECT_EQ(*mock_node_a_->OutputExpressions().at(1),
            *std::make_shared<LqpColumnExpression>(mock_node_a_, ColumnId{2}));
}

TEST_F(MockNodeTest, HashingAndEqualityCheck) {
  const auto same_mock_node_b =
      MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kFloat, "b"}}, "mock_name");
  const auto different_mock_node_1 =
      MockNode::Make(MockNode::ColumnDefinitions{{DataType::kLong, "a"}, {DataType::kString, "b"}}, "mock_name");
  const auto different_mock_node_2 =
      MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kFloat, "b"}}, "other_name");
  EXPECT_EQ(*mock_node_b_, *mock_node_b_);
  EXPECT_NE(*mock_node_b_, *different_mock_node_1);
  EXPECT_NE(*mock_node_b_, *different_mock_node_2);
  EXPECT_EQ(*mock_node_b_, *same_mock_node_b);

  EXPECT_NE(mock_node_b_->Hash(), different_mock_node_1->Hash());
  EXPECT_EQ(mock_node_b_->Hash(), different_mock_node_2->Hash());
  EXPECT_EQ(mock_node_b_->Hash(), same_mock_node_b->Hash());
}

TEST_F(MockNodeTest, Copy) {
  const auto copy = mock_node_b_->DeepCopy();
  EXPECT_EQ(*mock_node_b_, *copy);

  mock_node_b_->SetPrunedColumnIds({ColumnId{1}});
  EXPECT_NE(*mock_node_b_, *copy);
  EXPECT_EQ(*mock_node_b_, *mock_node_b_->DeepCopy());
}

TEST_F(MockNodeTest, NodeExpressions) { ASSERT_EQ(mock_node_a_->node_expressions_.size(), 0u); }

TEST_F(MockNodeTest, UniqueConstraints) {
  // Add constraints to MockNode
  const TableKeyConstraint key_constraint_a_b({ColumnId{0}, ColumnId{1}}, KeyConstraintType::kPrimaryKey);
  const TableKeyConstraint key_constraint_c({ColumnId{2}}, KeyConstraintType::kUnique);
  const auto table_key_constraints = TableKeyConstraints{key_constraint_a_b, key_constraint_c};
  mock_node_a_->set_key_constraints(table_key_constraints);

  // Basic checks
  const auto& unique_constraints_mock_node_a = mock_node_a_->UniqueConstraints();
  EXPECT_EQ(unique_constraints_mock_node_a->size(), 2);
  EXPECT_TRUE(mock_node_b_->UniqueConstraints()->empty());

  // In-depth verification
  EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(key_constraint_a_b, unique_constraints_mock_node_a));
  EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(key_constraint_c, unique_constraints_mock_node_a));

  // Check whether MockNode is referenced by the constraint's expressions
  for (const auto& unique_constraint : *unique_constraints_mock_node_a) {
    for (const auto& expression : unique_constraint.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LqpColumnExpression>(expression);
      EXPECT_TRUE(column_expression && !column_expression->original_node_.expired());
      EXPECT_TRUE(column_expression->original_node_.lock() == mock_node_a_);
    }
  }
}

TEST_F(MockNodeTest, UniqueConstraintsPrunedColumns) {
  // Prepare unique constraints
  const TableKeyConstraint key_constraint_a({ColumnId{0}}, KeyConstraintType::kUnique);
  const TableKeyConstraint key_constraint_a_b({ColumnId{0}, ColumnId{1}}, KeyConstraintType::kUnique);
  const TableKeyConstraint key_constraint_c({ColumnId{2}}, KeyConstraintType::kUnique);
  mock_node_a_->set_key_constraints({key_constraint_a, key_constraint_a_b, key_constraint_c});
  EXPECT_EQ(mock_node_a_->key_constraints().size(), 3);
  auto unique_constraints = mock_node_a_->UniqueConstraints();
  EXPECT_EQ(unique_constraints->size(), 3);

  // Prune column a, which should remove two unique constraints
  mock_node_a_->SetPrunedColumnIds({ColumnId{0}});

  // Basic check
  unique_constraints = mock_node_a_->UniqueConstraints();
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(key_constraint_c, unique_constraints));
}

}  // namespace skyrise
