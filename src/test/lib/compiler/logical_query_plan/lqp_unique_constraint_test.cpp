/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/functional_dependency.hpp"
#include "compiler/logical_query_plan/mock_node.hpp"
#include "gtest/gtest.h"

namespace skyrise {

class LqpUniqueConstraintTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_node_a_ =
        MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}},
                       "mock_node_a");
    a_ = mock_node_a_->get_column("a");
    b_ = mock_node_a_->get_column("b");
    c_ = mock_node_a_->get_column("c");

    mock_node_b_ =
        MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "x"}, {DataType::kInt, "y"}}, "mock_node_b");
    x_ = mock_node_b_->get_column("x");
    y_ = mock_node_b_->get_column("y");
  }

 protected:
  std::shared_ptr<MockNode> mock_node_a_, mock_node_b_;
  std::shared_ptr<LqpColumnExpression> a_, b_, c_, x_, y_;
};

TEST_F(LqpUniqueConstraintTest, Equals) {
  const auto unique_constraint_a = LqpUniqueConstraint({a_});
  const auto unique_constraint_a_b_c = LqpUniqueConstraint({a_, b_, c_});

  // Equal
  EXPECT_EQ(unique_constraint_a, LqpUniqueConstraint({a_}));
  EXPECT_EQ(unique_constraint_a_b_c, LqpUniqueConstraint({a_, b_, c_}));
  EXPECT_EQ(unique_constraint_a_b_c, LqpUniqueConstraint({b_, a_, c_}));
  EXPECT_EQ(unique_constraint_a_b_c, LqpUniqueConstraint({b_, c_, a_}));
  // Not Equal
  EXPECT_NE(unique_constraint_a, LqpUniqueConstraint({a_, b_}));
  EXPECT_NE(unique_constraint_a, LqpUniqueConstraint({b_}));
  EXPECT_NE(unique_constraint_a_b_c, LqpUniqueConstraint({a_, b_}));
  EXPECT_NE(unique_constraint_a_b_c, LqpUniqueConstraint({a_, b_, c_, x_}));
}

TEST_F(LqpUniqueConstraintTest, Hash) {
  const auto unique_constraint_a = LqpUniqueConstraint({a_});
  const auto unique_constraint_a_b_c = LqpUniqueConstraint({a_, b_, c_});

  // Equal Hash
  EXPECT_EQ(unique_constraint_a.hash(), LqpUniqueConstraint({a_}).hash());
  EXPECT_EQ(unique_constraint_a_b_c.hash(), LqpUniqueConstraint({a_, b_, c_}).hash());
  EXPECT_EQ(unique_constraint_a_b_c.hash(), LqpUniqueConstraint({c_, a_, b_}).hash());
  EXPECT_EQ(unique_constraint_a_b_c.hash(), LqpUniqueConstraint({c_, b_, a_}).hash());

  // Non-Equal Hash
  EXPECT_NE(unique_constraint_a.hash(), LqpUniqueConstraint({a_, b_}).hash());
  EXPECT_NE(unique_constraint_a.hash(), LqpUniqueConstraint({b_}).hash());
  EXPECT_NE(unique_constraint_a_b_c.hash(), LqpUniqueConstraint({a_, b_}).hash());
  EXPECT_NE(unique_constraint_a_b_c.hash(), LqpUniqueConstraint({a_, b_, c_, x_}).hash());
}

}  // namespace skyrise
