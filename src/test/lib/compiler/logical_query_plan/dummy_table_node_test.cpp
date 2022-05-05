/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/dummy_table_node.hpp"

#include <memory>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_utils.hpp"

namespace skyrise {

class DummyTableNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { dummy_table_node_ = DummyTableNode::Make(); }

  std::shared_ptr<DummyTableNode> dummy_table_node_;
};

TEST_F(DummyTableNodeTest, Description) { EXPECT_EQ(dummy_table_node_->Description(), "[DummyTable]"); }

TEST_F(DummyTableNodeTest, OutputColumnExpressions) { EXPECT_EQ(dummy_table_node_->OutputExpressions().size(), 0u); }

TEST_F(DummyTableNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*dummy_table_node_, *dummy_table_node_);
  EXPECT_EQ(*dummy_table_node_, *DummyTableNode::Make());

  EXPECT_EQ(dummy_table_node_->Hash(), dummy_table_node_->Hash());
  EXPECT_EQ(dummy_table_node_->Hash(), DummyTableNode::Make()->Hash());
}

TEST_F(DummyTableNodeTest, Copy) { EXPECT_EQ(*dummy_table_node_->DeepCopy(), *DummyTableNode::Make()); }

TEST_F(DummyTableNodeTest, NodeExpressions) { ASSERT_EQ(dummy_table_node_->node_expressions_.size(), 0u); }

}  // namespace skyrise
