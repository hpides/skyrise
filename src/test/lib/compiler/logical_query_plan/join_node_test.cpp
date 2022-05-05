/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/join_node.hpp"

#include <memory>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_utils.hpp"
#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "constraint_test_utils.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class JoinNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_node_a_ = MockNode::Make(
        MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}}, "t_a");
    mock_node_b_ = MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "x"}, {DataType::kFloat, "y"}}, "t_b");

    t_a_a_ = mock_node_a_->get_column("a");
    t_a_b_ = mock_node_a_->get_column("b");
    t_a_c_ = mock_node_a_->get_column("c");
    t_b_x_ = mock_node_b_->get_column("x");
    t_b_y_ = mock_node_b_->get_column("y");

    cross_join_node_ = JoinNode::Make(JoinMode::kCross, mock_node_a_, mock_node_b_);
    inner_join_node_ = JoinNode::Make(JoinMode::kInner, Equals_(t_a_a_, t_b_y_), mock_node_a_, mock_node_b_);
    semi_join_node_ = JoinNode::Make(JoinMode::kSemi, Equals_(t_a_a_, t_b_y_), mock_node_a_, mock_node_b_);
    anti_join_node_ = JoinNode::Make(JoinMode::kAntiNullAsTrue, Equals_(t_a_a_, t_b_y_), mock_node_a_, mock_node_b_);

    // Prepare constraint definitions
    key_constraint_a_ = TableKeyConstraint{{t_a_a_->original_column_id_}, KeyConstraintType::kUnique};
    key_constraint_b_c_ =
        TableKeyConstraint{{t_a_b_->original_column_id_, t_a_c_->original_column_id_}, KeyConstraintType::kUnique};
    key_constraint_x_ = TableKeyConstraint{{t_b_x_->original_column_id_}, KeyConstraintType::kUnique};
    key_constraint_y_ = TableKeyConstraint{{t_b_y_->original_column_id_}, KeyConstraintType::kUnique};
  }

  std::shared_ptr<MockNode> mock_node_a_;
  std::shared_ptr<MockNode> mock_node_b_;
  std::shared_ptr<JoinNode> inner_join_node_;
  std::shared_ptr<JoinNode> semi_join_node_;
  std::shared_ptr<JoinNode> anti_join_node_;
  std::shared_ptr<JoinNode> cross_join_node_;
  std::shared_ptr<LqpColumnExpression> t_a_a_;
  std::shared_ptr<LqpColumnExpression> t_a_b_;
  std::shared_ptr<LqpColumnExpression> t_a_c_;
  std::shared_ptr<LqpColumnExpression> t_b_x_;
  std::shared_ptr<LqpColumnExpression> t_b_y_;
  std::optional<TableKeyConstraint> key_constraint_a_;
  std::optional<TableKeyConstraint> key_constraint_b_c_;
  std::optional<TableKeyConstraint> key_constraint_x_;
  std::optional<TableKeyConstraint> key_constraint_y_;
};

TEST_F(JoinNodeTest, Description) { EXPECT_EQ(cross_join_node_->Description(), "[Join] Mode: Cross"); }

TEST_F(JoinNodeTest, DescriptionInnerJoin) { EXPECT_EQ(inner_join_node_->Description(), "[Join] Mode: Inner [a = y]"); }

TEST_F(JoinNodeTest, DescriptionSemiJoin) { EXPECT_EQ(semi_join_node_->Description(), "[Join] Mode: Semi [a = y]"); }

TEST_F(JoinNodeTest, DescriptionAntiJoin) {
  EXPECT_EQ(anti_join_node_->Description(), "[Join] Mode: AntiNullAsTrue [a = y]");
}

TEST_F(JoinNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(cross_join_node_->OutputExpressions().size(), 5u);
  EXPECT_EQ(*cross_join_node_->OutputExpressions().at(0), *t_a_a_);
  EXPECT_EQ(*cross_join_node_->OutputExpressions().at(1), *t_a_b_);
  EXPECT_EQ(*cross_join_node_->OutputExpressions().at(2), *t_a_c_);
  EXPECT_EQ(*cross_join_node_->OutputExpressions().at(3), *t_b_x_);
  EXPECT_EQ(*cross_join_node_->OutputExpressions().at(4), *t_b_y_);
}

TEST_F(JoinNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*cross_join_node_, *cross_join_node_);
  EXPECT_EQ(*inner_join_node_, *inner_join_node_);
  EXPECT_EQ(*semi_join_node_, *semi_join_node_);
  EXPECT_EQ(*anti_join_node_, *anti_join_node_);

  const auto other_join_node_a = JoinNode::Make(JoinMode::kInner, Equals_(t_a_a_, t_b_x_), mock_node_a_, mock_node_b_);
  const auto other_join_node_b = JoinNode::Make(JoinMode::kInner, NotLike_(t_a_a_, t_b_y_), mock_node_a_, mock_node_b_);
  const auto other_join_node_c = JoinNode::Make(JoinMode::kCross, mock_node_a_, mock_node_b_);
  const auto other_join_node_d = JoinNode::Make(JoinMode::kInner, Equals_(t_a_a_, t_b_y_), mock_node_a_, mock_node_b_);

  EXPECT_NE(*other_join_node_a, *inner_join_node_);
  EXPECT_NE(*other_join_node_b, *inner_join_node_);
  EXPECT_NE(*other_join_node_c, *inner_join_node_);
  EXPECT_EQ(*other_join_node_d, *inner_join_node_);

  EXPECT_NE(other_join_node_a->Hash(), inner_join_node_->Hash());
  EXPECT_NE(other_join_node_b->Hash(), inner_join_node_->Hash());
  EXPECT_NE(other_join_node_c->Hash(), inner_join_node_->Hash());
  EXPECT_EQ(other_join_node_d->Hash(), inner_join_node_->Hash());
}

TEST_F(JoinNodeTest, Copy) {
  EXPECT_EQ(*cross_join_node_, *cross_join_node_->DeepCopy());
  EXPECT_EQ(*inner_join_node_, *inner_join_node_->DeepCopy());
  EXPECT_EQ(*semi_join_node_, *semi_join_node_->DeepCopy());
  EXPECT_EQ(*anti_join_node_, *anti_join_node_->DeepCopy());
}

TEST_F(JoinNodeTest, OutputColumnExpressionsSemiJoin) {
  ASSERT_EQ(semi_join_node_->OutputExpressions().size(), 3u);
  EXPECT_EQ(*semi_join_node_->OutputExpressions().at(0), *t_a_a_);
  EXPECT_EQ(*semi_join_node_->OutputExpressions().at(1), *t_a_b_);
  EXPECT_EQ(*semi_join_node_->OutputExpressions().at(2), *t_a_c_);
}

TEST_F(JoinNodeTest, OutputColumnExpressionsAntiJoin) {
  ASSERT_EQ(anti_join_node_->OutputExpressions().size(), 3u);
  EXPECT_EQ(*anti_join_node_->OutputExpressions().at(0), *t_a_a_);
  EXPECT_EQ(*anti_join_node_->OutputExpressions().at(1), *t_a_b_);
  EXPECT_EQ(*anti_join_node_->OutputExpressions().at(2), *t_a_c_);
}

TEST_F(JoinNodeTest, NodeExpressions) {
  ASSERT_EQ(inner_join_node_->node_expressions_.size(), 1u);
  EXPECT_EQ(*inner_join_node_->node_expressions_.at(0u), *Equals_(t_a_a_, t_b_y_));
  ASSERT_EQ(cross_join_node_->node_expressions_.size(), 0u);
}

TEST_F(JoinNodeTest, IsColumnNullableWithoutOuterJoin) {
  // Test that for LQPs without (Left,Right)Outer Joins, lqp_column_is_nullable() is equivalent to
  // expression.is_nullable()

  // clang-format off
  const auto lqp =
  JoinNode::Make(JoinMode::kInner, Equals_(Add_(t_a_a_, Null_()), t_b_x_),
    ProjectionNode::Make(ExpressionVector_(t_a_a_, t_a_b_, Add_(t_a_a_, Null_())),
      mock_node_a_),
    mock_node_b_);
  // clang-format on

  EXPECT_FALSE(lqp->IsColumnNullable(ColumnId{0}));
  EXPECT_FALSE(lqp->IsColumnNullable(ColumnId{1}));
  EXPECT_TRUE(lqp->IsColumnNullable(ColumnId{2}));
  EXPECT_FALSE(lqp->IsColumnNullable(ColumnId{3}));
  EXPECT_FALSE(lqp->IsColumnNullable(ColumnId{4}));
}

TEST_F(JoinNodeTest, IsColumnNullableWithOuterJoin) {
  // Test that columns on the "null-supplying" side of an outer join are always nullable.
  // Test that IsNull_(<nullable>) is never nullable

  // clang-format off
  const auto lqp_left_join_basic =
  JoinNode::Make(JoinMode::kLeftOuter, Equals_(t_a_a_, t_b_x_),
    mock_node_a_,
    mock_node_b_);
  // clang-format on

  EXPECT_FALSE(lqp_left_join_basic->IsColumnNullable(ColumnId{0}));
  EXPECT_FALSE(lqp_left_join_basic->IsColumnNullable(ColumnId{1}));
  EXPECT_FALSE(lqp_left_join_basic->IsColumnNullable(ColumnId{2}));
  EXPECT_TRUE(lqp_left_join_basic->IsColumnNullable(ColumnId{3}));
  EXPECT_TRUE(lqp_left_join_basic->IsColumnNullable(ColumnId{4}));

  // clang-format off
  const auto lqp_left_join =
  ProjectionNode::Make(ExpressionVector_(t_a_a_, t_b_x_, Add_(t_a_a_, t_b_x_), Add_(t_a_a_, 3), IsNull_(Add_(t_a_a_, t_b_x_))),  // NOLINT
    JoinNode::Make(JoinMode::kLeftOuter, Equals_(t_a_a_, t_b_x_),
      mock_node_a_,
      mock_node_b_));
  // clang-format on

  EXPECT_FALSE(lqp_left_join->IsColumnNullable(ColumnId{0}));
  EXPECT_TRUE(lqp_left_join->IsColumnNullable(ColumnId{1}));
  EXPECT_TRUE(lqp_left_join->IsColumnNullable(ColumnId{2}));
  EXPECT_FALSE(lqp_left_join->IsColumnNullable(ColumnId{3}));
  EXPECT_FALSE(lqp_left_join->IsColumnNullable(ColumnId{4}));

  // clang-format off
  const auto lqp_right_join =
  ProjectionNode::Make(ExpressionVector_(t_a_a_, t_b_x_, Add_(t_a_a_, t_b_x_), Add_(t_a_a_, 3), IsNull_(Add_(t_a_a_, t_b_x_))),  // NOLINT
    JoinNode::Make(JoinMode::kRightOuter, Equals_(t_a_a_, t_b_x_),
      mock_node_a_,
      mock_node_b_));
  // clang-format on

  EXPECT_TRUE(lqp_right_join->IsColumnNullable(ColumnId{0}));
  EXPECT_FALSE(lqp_right_join->IsColumnNullable(ColumnId{1}));
  EXPECT_TRUE(lqp_right_join->IsColumnNullable(ColumnId{2}));
  EXPECT_TRUE(lqp_right_join->IsColumnNullable(ColumnId{3}));
  EXPECT_FALSE(lqp_right_join->IsColumnNullable(ColumnId{4}));

  // clang-format off
  const auto lqp_full_join =
  ProjectionNode::Make(ExpressionVector_(t_a_a_, t_b_x_, Add_(t_a_a_, t_b_x_), Add_(t_a_a_, 3), IsNull_(Add_(t_a_a_, t_b_x_))),  // NOLINT
    JoinNode::Make(JoinMode::kFullOuter, Equals_(t_a_a_, t_b_x_),
      mock_node_a_,
      mock_node_b_));
  // clang-format on

  EXPECT_TRUE(lqp_full_join->IsColumnNullable(ColumnId{0}));
  EXPECT_TRUE(lqp_full_join->IsColumnNullable(ColumnId{1}));
  EXPECT_TRUE(lqp_full_join->IsColumnNullable(ColumnId{2}));
  EXPECT_TRUE(lqp_full_join->IsColumnNullable(ColumnId{3}));
  EXPECT_FALSE(lqp_full_join->IsColumnNullable(ColumnId{4}));
}

TEST_F(JoinNodeTest, FunctionalDependenciesSemiAndAntiJoins) {
  // Preparations
  const FunctionalDependency fd_a({t_a_a_}, {t_a_b_});
  const FunctionalDependency fd_x({t_b_x_}, {t_b_y_});
  mock_node_a_->set_non_trivial_functional_dependencies({fd_a});
  EXPECT_EQ(mock_node_a_->NonTrivialFunctionalDependencies().size(), 1);
  EXPECT_EQ(mock_node_a_->NonTrivialFunctionalDependencies().at(0), fd_a);
  mock_node_b_->set_non_trivial_functional_dependencies({fd_x});
  EXPECT_EQ(mock_node_b_->NonTrivialFunctionalDependencies().size(), 1);
  EXPECT_EQ(mock_node_b_->NonTrivialFunctionalDependencies().at(0), fd_x);

  // Tests
  for (const auto join_mode : {JoinMode::kSemi, JoinMode::kAntiNullAsTrue, JoinMode::kAntiNullAsFalse}) {
    // clang-format off
    const auto join_node =
        JoinNode::Make(join_mode, Equals_(t_a_a_, t_b_y_),
                       mock_node_a_,
                       mock_node_b_);
    // clang-format on

    // We do not want JoinNode to return the FDs of the right input node
    const auto& non_trivial_fds = join_node->NonTrivialFunctionalDependencies();
    EXPECT_EQ(non_trivial_fds.size(), 1);
    EXPECT_EQ(non_trivial_fds.at(0), fd_a);
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardNonTrivialLeft) {
  for (const auto join_mode :
       {JoinMode::kInner, JoinMode::kLeftOuter, JoinMode::kRightOuter, JoinMode::kFullOuter, JoinMode::kCross}) {
    auto join_node = std::shared_ptr<JoinNode>();
    if (join_mode == JoinMode::kCross) {
      join_node = JoinNode::Make(JoinMode::kCross, mock_node_a_, mock_node_b_);
    } else {
      join_node = JoinNode::Make(join_mode, Equals_(t_a_a_, t_b_x_), mock_node_a_, mock_node_b_);
    }

    // Left input Node has non-trivial FDs
    const FunctionalDependency fd_a({t_a_a_}, {t_a_b_});
    mock_node_a_->set_non_trivial_functional_dependencies({fd_a});
    mock_node_b_->set_non_trivial_functional_dependencies({});
    EXPECT_TRUE(mock_node_a_->UniqueConstraints()->empty());
    EXPECT_TRUE(mock_node_b_->UniqueConstraints()->empty());

    if (join_mode == JoinMode::kRightOuter || join_mode == JoinMode::kFullOuter) {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 0);
    } else {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 1);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_a);
    }
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardNonTrivialRight) {
  for (const auto join_mode :
       {JoinMode::kInner, JoinMode::kLeftOuter, JoinMode::kRightOuter, JoinMode::kFullOuter, JoinMode::kCross}) {
    auto join_node = std::shared_ptr<JoinNode>();
    if (join_mode == JoinMode::kCross) {
      join_node = JoinNode::Make(JoinMode::kCross, mock_node_a_, mock_node_b_);
    } else {
      join_node = JoinNode::Make(join_mode, Equals_(t_a_a_, t_b_x_), mock_node_a_, mock_node_b_);
    }

    // Right input Node has non-trivial FDs
    const FunctionalDependency fd_x({t_b_x_}, {t_b_y_});
    mock_node_a_->set_non_trivial_functional_dependencies({});
    mock_node_b_->set_non_trivial_functional_dependencies({fd_x});
    EXPECT_TRUE(mock_node_a_->UniqueConstraints()->empty());
    EXPECT_TRUE(mock_node_b_->UniqueConstraints()->empty());

    if (join_mode == JoinMode::kLeftOuter || join_mode == JoinMode::kFullOuter) {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 0);
    } else {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 1);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_x);
    }
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardNonTrivialBoth) {
  for (const auto join_mode :
       {JoinMode::kInner, JoinMode::kLeftOuter, JoinMode::kRightOuter, JoinMode::kFullOuter, JoinMode::kCross}) {
    auto join_node = std::shared_ptr<JoinNode>();
    if (join_mode == JoinMode::kCross) {
      join_node = JoinNode::Make(JoinMode::kCross, mock_node_a_, mock_node_b_);
    } else {
      join_node = JoinNode::Make(join_mode, Equals_(t_a_a_, t_b_x_), mock_node_a_, mock_node_b_);
    }

    // Both input nodes have non-trivial FDs
    const FunctionalDependency fd_a({t_a_a_}, {t_a_b_});
    const FunctionalDependency fd_x({t_b_x_}, {t_b_y_});
    mock_node_a_->set_non_trivial_functional_dependencies({fd_a});
    mock_node_b_->set_non_trivial_functional_dependencies({fd_x});
    EXPECT_TRUE(mock_node_a_->UniqueConstraints()->empty());
    EXPECT_TRUE(mock_node_b_->UniqueConstraints()->empty());

    if (join_mode == JoinMode::kFullOuter) {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 0);
    } else if (join_mode == JoinMode::kLeftOuter) {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 1);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_a);
    } else if (join_mode == JoinMode::kRightOuter) {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 1);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_x);
    } else {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 2);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_a);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(1), fd_x);
    }
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesForwardNonTrivialBothAndDerive) {
  for (const auto join_mode :
       {JoinMode::kInner, JoinMode::kLeftOuter, JoinMode::kRightOuter, JoinMode::kFullOuter, JoinMode::kCross}) {
    auto join_node = std::shared_ptr<JoinNode>();
    if (join_mode == JoinMode::kCross) {
      join_node = JoinNode::Make(JoinMode::kCross, mock_node_a_, mock_node_b_);
    } else {
      join_node = JoinNode::Make(join_mode, Equals_(t_a_a_, t_b_x_), mock_node_a_, mock_node_b_);
    }

    /**
     * Test, whether non-trivial FD forwarding still works when deriving FDs from unique constraints:
     *  - We specify unique constraints for both input tables.
     *  - We enforce the dismissal of unique constraints for all join modes by making none of the join columns unique.
     *    Consequently, we expect non-trivial FDs, which were derived from the input nodes' unique constraints.
     */
    const FunctionalDependency fd_a({t_a_a_}, {t_a_b_});
    const FunctionalDependency fd_x({t_b_x_}, {t_b_y_});
    mock_node_a_->set_non_trivial_functional_dependencies({fd_a});
    mock_node_b_->set_non_trivial_functional_dependencies({fd_x});
    mock_node_a_->set_key_constraints({*key_constraint_b_c_});
    mock_node_b_->set_key_constraints({*key_constraint_y_});
    const FunctionalDependency generated_fd_b_c({t_a_b_, t_a_c_}, {t_a_a_});
    const FunctionalDependency generated_fd_y({t_b_y_}, {t_b_x_});

    if (join_mode == JoinMode::kFullOuter) {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 0);
    } else if (join_mode == JoinMode::kLeftOuter) {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 2);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_a);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(1), generated_fd_b_c);
    } else if (join_mode == JoinMode::kRightOuter) {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 2);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_x);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(1), generated_fd_y);
    } else {
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 4);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_a);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(1), generated_fd_b_c);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(2), fd_x);
      EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(3), generated_fd_y);
    }
  }
}

TEST_F(JoinNodeTest, FunctionalDependenciesDeriveNone) {
  /**
   * Set unique constraints for both join columns of an Inner Join, so that unique constraints from both sides become
   * forwarded. Consequently, we do not expect non-trivial FDs from the left or right input node's unique constraints
   * to be derived.
   */
  mock_node_a_->set_key_constraints({*key_constraint_a_});
  mock_node_b_->set_key_constraints({*key_constraint_x_});

  // MockNodes with non-trivial FDs
  const FunctionalDependency fd_b({t_a_b_}, {t_a_a_});
  const FunctionalDependency fd_y({t_b_y_}, {t_b_x_});
  mock_node_a_->set_non_trivial_functional_dependencies({fd_b});
  mock_node_b_->set_non_trivial_functional_dependencies({fd_y});

  // clang-format off
  const auto& join_node =
  JoinNode::Make(JoinMode::kInner, Equals_(t_a_a_, t_b_x_),
    mock_node_a_,
    mock_node_b_);
  // clang-format on

  // Tests
  EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 2);
  EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), fd_b);
  EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(1), fd_y);

  EXPECT_EQ(join_node->FunctionalDependencies().size(), 4);
  EXPECT_EQ(join_node->FunctionalDependencies().at(0), fd_b);
  EXPECT_EQ(join_node->FunctionalDependencies().at(1), fd_y);
  const FunctionalDependency generated_fd_a({t_a_a_}, {t_a_b_, t_a_c_, t_b_x_, t_b_y_});
  EXPECT_EQ(join_node->FunctionalDependencies().at(2), generated_fd_a);
  const FunctionalDependency generated_fd_x({t_b_x_}, {t_b_y_, t_a_a_, t_a_b_, t_a_c_});
  EXPECT_EQ(join_node->FunctionalDependencies().at(3), generated_fd_x);
}

TEST_F(JoinNodeTest, FunctionalDependenciesDeriveLeftOnly) {
  /**
   * We set a unique constraint for the left, but not for the right join column of the Inner Join. Consequently, unique
   * constraints of the left input node become discarded whereas the unique constraints of the right input node survive.
   * Therefore, we have to check whether left input node's trivial FDs become forwarded as non-trivial ones.
   */
  mock_node_a_->set_key_constraints({*key_constraint_a_});
  mock_node_b_->set_key_constraints({*key_constraint_x_});
  // clang-format off
  const auto& join_node =
  JoinNode::Make(JoinMode::kInner, Equals_(t_a_a_, t_b_y_),
    mock_node_a_,
    mock_node_b_);
  // clang-format on

  // Tests
  const FunctionalDependency generated_fd_a({t_a_a_}, {t_a_b_, t_a_c_});
  EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().size(), 1);
  EXPECT_EQ(join_node->NonTrivialFunctionalDependencies().at(0), generated_fd_a);

  const FunctionalDependency generated_fd_x({t_b_x_}, {t_a_a_, t_a_b_, t_a_c_, t_b_y_});
  EXPECT_EQ(join_node->FunctionalDependencies().size(), 2);
  EXPECT_EQ(join_node->FunctionalDependencies().at(0), generated_fd_a);
  EXPECT_EQ(join_node->FunctionalDependencies().at(1), generated_fd_x);
}

TEST_F(JoinNodeTest, FunctionalDependenciesUnify) {
  const auto key_constraint_a_b =
      TableKeyConstraint{{t_a_a_->original_column_id_, t_a_b_->original_column_id_}, KeyConstraintType::kPrimaryKey};
  const auto key_constraint_c = TableKeyConstraint{{t_a_c_->original_column_id_}, KeyConstraintType::kUnique};
  mock_node_a_->set_key_constraints({key_constraint_a_b, key_constraint_c});
  mock_node_b_->set_key_constraints({*key_constraint_x_});

  // The following FD is trivial since it can be derived from a unique constraint (PRIMARY KEY across a & b).
  // However, we define it as non-trivial anyway, to verify the conflict resolution when merging FDs later on.
  const FunctionalDependency fd_a_b({t_a_a_, t_a_b_}, {t_a_c_});
  mock_node_a_->set_non_trivial_functional_dependencies({fd_a_b});

  // Define an Inner Join, so that all unique constraints survive
  // clang-format off
  const auto& join_node =
  JoinNode::Make(JoinMode::kInner, Equals_(t_a_c_, t_b_x_),
    mock_node_a_,
    mock_node_b_);
  // clang-format on

  // After the join, we expect the following FDs to be returned:
  const FunctionalDependency expected_fd_a_b({t_a_a_, t_a_b_}, {t_a_c_, t_b_x_, t_b_y_});
  const FunctionalDependency expected_fd_c({t_a_c_}, {t_a_a_, t_a_b_, t_b_x_, t_b_y_});
  const FunctionalDependency expected_fd_x({t_b_x_}, {t_a_a_, t_a_b_, t_a_c_, t_b_y_});

  // Prerequisites
  const auto& non_trivial_fds = join_node->NonTrivialFunctionalDependencies();
  EXPECT_EQ(non_trivial_fds.size(), 1);
  EXPECT_EQ(non_trivial_fds.at(0), fd_a_b);

  const auto& trivial_fds = FdsFromUniqueConstraints(join_node, join_node->UniqueConstraints());
  EXPECT_EQ(trivial_fds.size(), 3);
  EXPECT_EQ(trivial_fds.at(0), expected_fd_a_b);
  EXPECT_EQ(trivial_fds.at(1), expected_fd_c);
  EXPECT_EQ(trivial_fds.at(2), expected_fd_x);

  /**
   * After unifiying the two FD sets above, we expect three and instead of four FDs since the following FD objects
   *   {a, b} => {c} and
   *   {a, b} => {c, x, y}
   * can be merged into one.
   */
  const auto fds_unified = UnionFds(non_trivial_fds, trivial_fds);
  EXPECT_EQ(fds_unified.size(), 3);
  const auto fds_unified_set = std::unordered_set<FunctionalDependency>(fds_unified.begin(), fds_unified.end());
  // TODO(julianmenzler) C++20: Replace with .contains
  EXPECT_TRUE(fds_unified_set.find(expected_fd_a_b) != fds_unified_set.end());
  EXPECT_TRUE(fds_unified_set.find(expected_fd_c) != fds_unified_set.end());
  EXPECT_TRUE(fds_unified_set.find(expected_fd_x) != fds_unified_set.end());
  EXPECT_EQ(fds_unified, join_node->FunctionalDependencies());
}

TEST_F(JoinNodeTest, UniqueConstraintsSemiAndAntiJoins) {
  mock_node_a_->set_key_constraints({*key_constraint_a_, *key_constraint_b_c_});
  mock_node_b_->set_key_constraints({*key_constraint_x_});

  for (const auto join_mode : {JoinMode::kSemi, JoinMode::kAntiNullAsTrue, JoinMode::kAntiNullAsFalse}) {
    // clang-format off
    const auto join_node =
    JoinNode::Make(join_mode, Equals_(t_a_a_, t_b_y_),
      mock_node_a_,
      mock_node_b_);
    // clang-format on

    EXPECT_EQ(*join_node->UniqueConstraints(), *mock_node_a_->UniqueConstraints());
  }
}

TEST_F(JoinNodeTest, UniqueConstraintsInnerAndOuterJoins) {
  // Test the forwarding logic of all, inner and outer joins based on join column uniqueness.
  // Any join column that is not unique might lead to row-/value-duplication in the opposite table. Hence, unique
  // constraints might break and should not be forwarded.
  // Since our current unique constraints implementation is compatible with NULL values, outer joins are handled like
  // inner joins.

  for (const auto join_mode : {JoinMode::kInner, JoinMode::kLeftOuter, JoinMode::kRightOuter, JoinMode::kFullOuter}) {
    // clang-format off
    const auto join_node =
    JoinNode::Make(join_mode, Equals_(t_a_a_, t_b_y_),
      mock_node_a_,
      mock_node_b_);
    // clang-format on

    // Case 1 – LEFT  table's join column (a) uniqueness : No
    //          RIGHT table's join column (y) uniqueness : No
    mock_node_a_->set_key_constraints({*key_constraint_b_c_});
    mock_node_b_->set_key_constraints({*key_constraint_x_});
    EXPECT_TRUE(join_node->UniqueConstraints()->empty());

    // Case 2 – LEFT  table's join column (a) uniqueness : Yes
    //          RIGHT table's join column (y) uniqueness : No
    mock_node_a_->set_key_constraints({*key_constraint_a_, *key_constraint_b_c_});
    mock_node_b_->set_key_constraints({*key_constraint_x_});

    // Expect unique constraints of RIGHT table to be forwarded
    auto join_unique_constraints = join_node->UniqueConstraints();
    EXPECT_EQ(join_unique_constraints->size(), 1);
    EXPECT_TRUE(*join_unique_constraints == *mock_node_b_->UniqueConstraints());

    // Case 3 – LEFT  table's join column (a) uniqueness : No
    //          RIGHT table's join column (y) uniqueness : Yes
    mock_node_a_->set_key_constraints({*key_constraint_b_c_});
    mock_node_b_->set_key_constraints({*key_constraint_x_, *key_constraint_y_});

    // Expect unique constraints of LEFT table (b_c) to be forwarded
    join_unique_constraints = join_node->UniqueConstraints();
    EXPECT_EQ(join_unique_constraints->size(), 1);
    EXPECT_TRUE(*join_unique_constraints == *mock_node_a_->UniqueConstraints());

    // Case 4 – LEFT  table's join column (a) uniqueness : Yes
    //          RIGHT table's join column (y) uniqueness : Yes
    mock_node_a_->set_key_constraints({*key_constraint_a_, *key_constraint_b_c_});
    mock_node_b_->set_key_constraints({*key_constraint_x_, *key_constraint_y_});

    // Expect unique constraints of both, LEFT (a, b_c) and RIGHT (x, y) table to be forwarded
    join_unique_constraints = join_node->UniqueConstraints();

    // Basic check
    EXPECT_EQ(join_unique_constraints->size(), 4);
    // In-depth checks
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_a_, join_unique_constraints));
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_b_c_, join_unique_constraints));
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_x_, join_unique_constraints));
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_y_, join_unique_constraints));
  }
}

TEST_F(JoinNodeTest, UniqueConstraintsNonEquiJoin) {
  // Currently, we do not support unique constraint forwarding for Non-Equi- or Theta-Joins
  mock_node_a_->set_key_constraints({*key_constraint_a_, *key_constraint_b_c_});
  mock_node_b_->set_key_constraints({*key_constraint_x_, *key_constraint_y_});
  // clang-format off
  const auto theta_join_node =
  JoinNode::Make(JoinMode::kInner, GreaterThan_(t_a_a_, t_b_x_),
    mock_node_a_,
    mock_node_b_);
  // clang-format on

  EXPECT_TRUE(theta_join_node->UniqueConstraints()->empty());
}

TEST_F(JoinNodeTest, UniqueConstraintsNonSemiNonAntiMultiPredicateJoin) {
  // Except for Semi- and Anti-Joins, we do not support forwarding of unique constraints for multi-predicate joins.
  mock_node_a_->set_key_constraints({*key_constraint_a_, *key_constraint_b_c_});
  mock_node_b_->set_key_constraints({*key_constraint_x_, *key_constraint_y_});
  // clang-format off
  const auto join_node =
  JoinNode::Make(JoinMode::kInner, ExpressionVector_(LessThan_(t_a_a_, t_b_x_), GreaterThan_(t_a_a_, t_b_y_)),
    mock_node_a_,
    mock_node_b_);
  // clang-format on

  EXPECT_TRUE(join_node->UniqueConstraints()->empty());
}

TEST_F(JoinNodeTest, UniqueConstraintsCrossJoin) {
  mock_node_a_->set_key_constraints({*key_constraint_a_, *key_constraint_b_c_});
  mock_node_b_->set_key_constraints({*key_constraint_x_, *key_constraint_y_});

  EXPECT_TRUE(cross_join_node_->UniqueConstraints()->empty());
}

}  // namespace skyrise
