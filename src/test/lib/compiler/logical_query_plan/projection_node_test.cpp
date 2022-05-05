/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/projection_node.hpp"

#include <memory>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/mock_node.hpp"
#include "constraint_test_utils.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "storage/table/table_key_constraint.hpp"
using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class ProjectionNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_node_ = MockNode::Make(
        MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}}, "t_a");

    a_ = mock_node_->get_column("a");
    b_ = mock_node_->get_column("b");
    c_ = mock_node_->get_column("c");

    // SELECT c, a, b, b+c, a+c
    projection_node_ = ProjectionNode::Make(ExpressionVector_(c_, a_, b_, Add_(b_, c_), Add_(a_, c_)), mock_node_);

    key_constraint_a_b_pk_ = TableKeyConstraint{{ColumnId{0}, ColumnId{1}}, KeyConstraintType::kPrimaryKey};
    key_constraint_b_ = TableKeyConstraint{{ColumnId{1}}, KeyConstraintType::kUnique};
  }

  std::optional<TableKeyConstraint> key_constraint_a_b_pk_;
  std::optional<TableKeyConstraint> key_constraint_b_;
  std::shared_ptr<MockNode> mock_node_;
  std::shared_ptr<ProjectionNode> projection_node_;
  std::shared_ptr<LqpColumnExpression> a_, b_, c_;
};

TEST_F(ProjectionNodeTest, Description) {
  EXPECT_EQ(projection_node_->Description(), "[Projection] c, a, b, b + c, a + c");
}

TEST_F(ProjectionNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*projection_node_, *projection_node_);

  const auto different_projection_node_a =
      ProjectionNode::Make(ExpressionVector_(a_, c_, b_, Add_(b_, c_), Add_(a_, c_)), mock_node_);
  const auto different_projection_node_b =
      ProjectionNode::Make(ExpressionVector_(c_, a_, b_, Add_(b_, c_)), mock_node_);
  EXPECT_NE(*projection_node_, *different_projection_node_a);
  EXPECT_NE(*projection_node_, *different_projection_node_b);

  EXPECT_NE(projection_node_->Hash(), different_projection_node_a->Hash());
  EXPECT_NE(projection_node_->Hash(), different_projection_node_b->Hash());
}

TEST_F(ProjectionNodeTest, Copy) { EXPECT_EQ(*projection_node_->DeepCopy(), *projection_node_); }

TEST_F(ProjectionNodeTest, NodeExpressions) {
  ASSERT_EQ(projection_node_->node_expressions_.size(), 5u);
  EXPECT_EQ(*projection_node_->node_expressions_.at(0), *c_);
  EXPECT_EQ(*projection_node_->node_expressions_.at(1), *a_);
  EXPECT_EQ(*projection_node_->node_expressions_.at(2), *b_);
  EXPECT_EQ(*projection_node_->node_expressions_.at(3), *Add_(b_, c_));
  EXPECT_EQ(*projection_node_->node_expressions_.at(4), *Add_(a_, c_));
}

TEST_F(ProjectionNodeTest, UniqueConstraintsEmpty) {
  EXPECT_TRUE(mock_node_->UniqueConstraints()->empty());
  EXPECT_TRUE(projection_node_->UniqueConstraints()->empty());
}

TEST_F(ProjectionNodeTest, UniqueConstraintsReorderedColumns) {
  // Add constraints to MockNode
  mock_node_->SetKeyConstraints({*key_constraint_a_b_pk_, *key_constraint_b_});
  EXPECT_EQ(mock_node_->UniqueConstraints()->size(), 2);

  {
    // Reorder columns: (a, b, c) -> (c, a, b)
    projection_node_ = ProjectionNode::Make(ExpressionVector_(c_, a_, b_), mock_node_);

    // Basic check
    const auto& unique_constraints = projection_node_->UniqueConstraints();
    EXPECT_EQ(unique_constraints->size(), 2);
    // In-depth check
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_a_b_pk_, unique_constraints));
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_b_, unique_constraints));
  }

  {
    // Reorder columns: (a, b, c) -> (b, c, a)
    projection_node_ = ProjectionNode::Make(ExpressionVector_(c_, a_, b_), mock_node_);

    // Basic check
    const auto& unique_constraints = projection_node_->UniqueConstraints();
    EXPECT_EQ(unique_constraints->size(), 2);
    // In-depth check
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_a_b_pk_, unique_constraints));
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_b_, unique_constraints));
  }
}

TEST_F(ProjectionNodeTest, UniqueConstraintsRemovedColumns) {
  // Prepare two unique constraints for MockNode
  mock_node_->SetKeyConstraints({*key_constraint_a_b_pk_, *key_constraint_b_});
  EXPECT_EQ(mock_node_->UniqueConstraints()->size(), 2);

  // Test (a, b, c) -> (a, c) - no more constraints valid
  projection_node_ = ProjectionNode::Make(ExpressionVector_(a_, c_), mock_node_);
  EXPECT_TRUE(projection_node_->UniqueConstraints()->empty());

  // Test (a, b, c) -> (c) - no more constraints valid
  projection_node_ = ProjectionNode::Make(ExpressionVector_(c_), mock_node_);
  EXPECT_TRUE(projection_node_->UniqueConstraints()->empty());

  {
    // Test (a, b, c) -> (a, b) - all constraints remain valid
    projection_node_ = ProjectionNode::Make(ExpressionVector_(a_, b_), mock_node_);

    // Basic check
    const auto& unique_constraints = projection_node_->UniqueConstraints();
    EXPECT_EQ(unique_constraints->size(), 2);
    // In-depth check
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_a_b_pk_, unique_constraints));
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_b_, unique_constraints));
  }

  {
    // Test (a, b, c) -> (b) - unique constraint for b remains valid
    projection_node_ = ProjectionNode::Make(ExpressionVector_(b_), mock_node_);

    // Basic check
    const auto& unique_constraints = projection_node_->UniqueConstraints();
    EXPECT_EQ(unique_constraints->size(), 1);
    // In-depth check
    EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(*key_constraint_b_, unique_constraints));
  }
}

TEST_F(ProjectionNodeTest, FunctionalDependenciesForwarding) {
  // Preparations
  const FunctionalDependency fd_a({a_}, {c_});
  const FunctionalDependency fd_b({b_}, {c_});
  const FunctionalDependency fd_b_two_dependent_expressions({b_}, {a_, c_});
  mock_node_->set_non_trivial_functional_dependencies({fd_a, fd_b, fd_b_two_dependent_expressions});
  EXPECT_EQ(mock_node_->FunctionalDependencies().size(), 3);

  // Tests
  // FDs without dependent_expressions are discarded
  const auto& projection_node_1 = ProjectionNode::Make(ExpressionVector_(a_, Add_(b_, c_)), mock_node_);
  EXPECT_TRUE(projection_node_1->FunctionalDependencies().empty());
  const auto& projection_node_2 = ProjectionNode::Make(ExpressionVector_(b_, Sub_(b_, c_)), mock_node_);
  EXPECT_TRUE(projection_node_2->FunctionalDependencies().empty());

  // Missing determinant_expressions lead to FD removal
  const auto& projection_node_3 = ProjectionNode::Make(ExpressionVector_(a_, c_), mock_node_);
  EXPECT_EQ(projection_node_3->FunctionalDependencies().size(), 1);
  EXPECT_EQ(projection_node_3->FunctionalDependencies().at(0), fd_a);

  // FDs are adjusted if some, but not all dependent_expressions are missing
  const auto& projection_node_4 = ProjectionNode::Make(ExpressionVector_(a_, b_), mock_node_);
  EXPECT_EQ(projection_node_4->FunctionalDependencies().size(), 1);
  const FunctionalDependency expected_fd({b_}, {a_});
  EXPECT_EQ(projection_node_4->FunctionalDependencies().at(0), expected_fd);
}

}  // namespace skyrise
