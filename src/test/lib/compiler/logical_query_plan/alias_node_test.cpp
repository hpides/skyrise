/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/alias_node.hpp"

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "compiler/logical_query_plan/mock_node.hpp"
#include "constraint_test_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "storage/table/table_key_constraint.hpp"

namespace skyrise {

class AliasNodeTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_node_ = MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kFloat, "b"}});
    a_ = mock_node_->get_column("a");
    b_ = mock_node_->get_column("b");

    aliases_ = {"x", "y"};
    expressions_ = {b_, a_};
    alias_node_ = AliasNode::Make(expressions_, aliases_, mock_node_);
  }

 protected:
  std::vector<std::string> aliases_;
  std::vector<std::shared_ptr<AbstractExpression>> expressions_;
  std::shared_ptr<MockNode> mock_node_;

  std::shared_ptr<AbstractExpression> a_, b_;
  std::shared_ptr<AliasNode> alias_node_;
};

TEST_F(AliasNodeTest, NodeExpressions) {
  ASSERT_EQ(alias_node_->node_expressions_.size(), 2u);
  EXPECT_EQ(alias_node_->node_expressions_.at(0), b_);
  EXPECT_EQ(alias_node_->node_expressions_.at(1), a_);
}

TEST_F(AliasNodeTest, ShallowEqualsAndCopy) {
  const auto alias_node_copy = alias_node_->DeepCopy();
  const auto node_mapping = lqp_create_node_mapping(alias_node_, alias_node_copy);

  EXPECT_TRUE(alias_node_->ShallowEquals(*alias_node_copy, node_mapping));
}

TEST_F(AliasNodeTest, HashingAndEqualityCheck) {
  const auto alias_node_copy = alias_node_->DeepCopy();
  EXPECT_EQ(*alias_node_, *alias_node_copy);

  const auto alias_node_other_aliases = AliasNode::Make(expressions_, std::vector<std::string>{"a", "b"}, mock_node_);
  EXPECT_NE(*alias_node_, *alias_node_other_aliases);

  const auto other_mock_node_ =
      MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kFloat, "b"}}, "named");
  const auto expr_a = other_mock_node_->get_column("a");
  const auto expr_b = other_mock_node_->get_column("b");
  const auto other_expressions = std::vector<std::shared_ptr<AbstractExpression>>{expr_a, expr_b};
  const auto alias_node_other_expressions = AliasNode::Make(other_expressions, aliases_, mock_node_);
  EXPECT_NE(*alias_node_, *alias_node_other_expressions);
  const auto alias_node_other_left_input = AliasNode::Make(expressions_, aliases_, other_mock_node_);
  EXPECT_NE(*alias_node_, *alias_node_other_left_input);

  EXPECT_NE(alias_node_->Hash(), alias_node_other_expressions->Hash());
  EXPECT_EQ(alias_node_->Hash(), alias_node_other_left_input->Hash());
  // alias_node_ == alias_node_other_left_input is false but the hash codes of these nodes are equal. The reason for
  // this is in the LqpColumnExpressions: Semantically equal LqpColumnExpressions are not equal if they refer to
  // different original_nodes. This allows, e.g., for self-joins. The hash function does not take the actual pointer
  // into account, so the hashes of semantically equal LqpColumnExpressions are equal. The following lines show this
  // fact in detail:
  EXPECT_NE(*a_, *expr_a);
  EXPECT_NE(*b_, *expr_b);
  EXPECT_EQ(a_->Hash(), expr_a->Hash());
  EXPECT_EQ(b_->Hash(), expr_b->Hash());
}

TEST_F(AliasNodeTest, UniqueConstraintsEmpty) {
  EXPECT_TRUE(mock_node_->UniqueConstraints()->empty());
  EXPECT_TRUE(alias_node_->UniqueConstraints()->empty());
}

TEST_F(AliasNodeTest, UniqueConstraintsForwarding) {
  // Add constraints to MockNode
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnId{0}, ColumnId{1}}, KeyConstraintType::kPrimaryKey};
  const auto key_constraint_b = TableKeyConstraint{{ColumnId{1}}, KeyConstraintType::kUnique};
  mock_node_->set_key_constraints({key_constraint_a_b, key_constraint_b});

  // Basic check
  const auto& unique_constraints = alias_node_->UniqueConstraints();
  EXPECT_EQ(unique_constraints->size(), 2);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_a_b, unique_constraints));
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_b, unique_constraints));
}

}  // namespace skyrise
