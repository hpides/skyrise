/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/limit_node.hpp"

#include <memory>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "expression/expression_functional.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class LimitNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { limit_node_ = LimitNode::Make(Value_(10)); }

  std::shared_ptr<LimitNode> limit_node_;
};

TEST_F(LimitNodeTest, Description) { EXPECT_EQ(limit_node_->Description(), "[Limit] 10"); }

TEST_F(LimitNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*limit_node_, *limit_node_);
  EXPECT_EQ(*LimitNode::Make(Value_(10)), *limit_node_);
  EXPECT_NE(*LimitNode::Make(Value_(11)), *limit_node_);

  EXPECT_EQ(LimitNode::Make(Value_(10))->Hash(), limit_node_->Hash());
  EXPECT_NE(LimitNode::Make(Value_(11))->Hash(), limit_node_->Hash());
}

TEST_F(LimitNodeTest, Copy) { EXPECT_EQ(*limit_node_->DeepCopy(), *limit_node_); }

TEST_F(LimitNodeTest, NodeExpressions) {
  ASSERT_EQ(limit_node_->node_expressions_.size(), 1u);
  EXPECT_EQ(*limit_node_->node_expressions_.at(0u), *Value_(10));
}

}  // namespace skyrise
