/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/union_node.hpp"

#include <memory>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/aggregate_node.hpp"
#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/logical_query_plan/predicate_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "expression/expression_functional.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class UnionNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_node1_ = MockNode::Make(
        MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}}, "t_a");
    a_ = mock_node1_->get_column("a");
    b_ = mock_node1_->get_column("b");
    c_ = mock_node1_->get_column("c");

    union_node_ = UnionNode::Make(SetOperationMode::kAll);
    union_node_->SetLeftInput(mock_node1_);
    union_node_->SetRightInput(mock_node1_);

    mock_node2_ = MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "u"}, {DataType::kInt, "v"}}, "t_b");
    u_ = mock_node2_->get_column("u");
    v_ = mock_node2_->get_column("v");
  }

  std::shared_ptr<MockNode> mock_node1_, mock_node2_;
  std::shared_ptr<UnionNode> union_node_;
  std::shared_ptr<LqpColumnExpression> a_;
  std::shared_ptr<LqpColumnExpression> b_;
  std::shared_ptr<LqpColumnExpression> c_;
  std::shared_ptr<LqpColumnExpression> u_;
  std::shared_ptr<LqpColumnExpression> v_;
};

TEST_F(UnionNodeTest, Description) { EXPECT_EQ(union_node_->Description(), "[UnionNode] Mode: All"); }

TEST_F(UnionNodeTest, OutputColumnExpressions) {
  EXPECT_EQ(*union_node_->OutputExpressions().at(0), *mock_node1_->OutputExpressions().at(0));
  EXPECT_EQ(*union_node_->OutputExpressions().at(1), *mock_node1_->OutputExpressions().at(1));
  EXPECT_EQ(*union_node_->OutputExpressions().at(2), *mock_node1_->OutputExpressions().at(2));
}

TEST_F(UnionNodeTest, HashingAndEqualityCheck) {
  auto same_union_node = UnionNode::Make(SetOperationMode::kAll);
  same_union_node->SetLeftInput(mock_node1_);
  same_union_node->SetRightInput(mock_node1_);
  auto different_union_node = UnionNode::Make(SetOperationMode::kUnique);
  different_union_node->SetLeftInput(mock_node1_);
  different_union_node->SetRightInput(mock_node1_);
  auto different_union_node_1 = UnionNode::Make(SetOperationMode::kUnique);
  different_union_node_1->SetLeftInput(mock_node1_);
  different_union_node_1->SetRightInput(mock_node2_);
  auto different_union_node_2 = UnionNode::Make(SetOperationMode::kUnique);
  different_union_node_2->SetLeftInput(mock_node2_);
  different_union_node_2->SetRightInput(mock_node1_);
  auto different_union_node_3 = UnionNode::Make(SetOperationMode::kUnique);
  different_union_node_3->SetLeftInput(mock_node2_);
  different_union_node_3->SetRightInput(mock_node2_);

  EXPECT_EQ(*union_node_, *same_union_node);
  EXPECT_NE(*union_node_, *different_union_node);
  EXPECT_NE(*union_node_, *different_union_node_1);
  EXPECT_NE(*union_node_, *different_union_node_2);
  EXPECT_NE(*union_node_, *different_union_node_3);
  EXPECT_NE(*union_node_, *UnionNode::Make(SetOperationMode::kUnique));
  EXPECT_NE(*union_node_, *UnionNode::Make(SetOperationMode::kAll));

  EXPECT_EQ(union_node_->hash(), same_union_node->hash());
  EXPECT_NE(union_node_->hash(), different_union_node->hash());
  EXPECT_NE(union_node_->hash(), different_union_node_1->hash());
  EXPECT_NE(union_node_->hash(), different_union_node_2->hash());
  EXPECT_NE(union_node_->hash(), different_union_node_3->hash());
  EXPECT_NE(union_node_->hash(), UnionNode::Make(SetOperationMode::kUnique)->hash());
  EXPECT_NE(union_node_->hash(), UnionNode::Make(SetOperationMode::kAll)->hash());
}

TEST_F(UnionNodeTest, Copy) { EXPECT_EQ(*union_node_->DeepCopy(), *union_node_); }

TEST_F(UnionNodeTest, NodeExpressions) { ASSERT_EQ(union_node_->node_expressions_.size(), 0u); }

TEST_F(UnionNodeTest, FunctionalDependenciesUnionAllSimple) {
  const auto trivial_fd_a = FunctionalDependency({a_}, {b_, c_});
  const auto non_trivial_fd_b = FunctionalDependency({b_}, {a_});
  const auto non_trivial_fd_c = FunctionalDependency({c_}, {b_});

  // Set FDs
  mock_node1_->set_key_constraints({{{a_->original_column_id_}, KeyConstraintType::UNIQUE}});
  mock_node1_->set_non_trivial_functional_dependencies({non_trivial_fd_b, non_trivial_fd_c});
  EXPECT_EQ(mock_node1_->FunctionalDependencies().size(), 3);
  EXPECT_EQ(mock_node1_->FunctionalDependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(mock_node1_->FunctionalDependencies().at(1), non_trivial_fd_c);
  EXPECT_EQ(mock_node1_->FunctionalDependencies().at(2), trivial_fd_a);

  // Create PredicateNodes & UnionAll
  const auto& predicate_node_a = PredicateNode::Make(GreaterThan_(a_, 5), mock_node1_);
  const auto& predicate_node_b = PredicateNode::Make(GreaterThan_(b_, 5), mock_node1_);
  const auto& union_all_node = UnionNode::Make(SetOperationMode::kAll);
  union_all_node->SetLeftInput(predicate_node_a);
  union_all_node->SetRightInput(predicate_node_b);

  // We expect all FDs to be forwarded since both input nodes have the same non-trivial FDs & unique constraints.
  const auto& union_node_fds = union_all_node->FunctionalDependencies();
  const auto& union_node_non_trivial_fds = union_all_node->NonTrivialFunctionalDependencies();
  // Since all unique constraints become discarded, former trivial FDs become non-trivial:
  EXPECT_EQ(union_node_fds, union_node_non_trivial_fds);

  EXPECT_EQ(union_node_fds.size(), 3);
  const auto& union_node_fds_set =
      std::unordered_set<FunctionalDependency>(union_node_fds.cbegin(), union_node_fds.cend());
  // TODO(julianmenzler) C++20: Replace with .contains
  EXPECT_TRUE(union_node_fds_set.find(trivial_fd_a) != union_node_fds_set.end());
  EXPECT_TRUE(union_node_fds_set.find(non_trivial_fd_b) != union_node_fds_set.end());
  EXPECT_TRUE(union_node_fds_set.find(non_trivial_fd_c) != union_node_fds_set.end());
}

TEST_F(UnionNodeTest, FunctionalDependenciesUnionAllIntersect) {
  // Create single non-trivial FD
  const auto non_trivial_fd_b = FunctionalDependency({a_}, {b_});
  mock_node1_->set_non_trivial_functional_dependencies({non_trivial_fd_b});

  /**
   * Create UnionNode
   * Hack: We use an AggregateNode with a pseudo-aggregate ANY(c_) to
   *        - receive a new unique constraint and also
   *        - a new trivial FD {a_, b_} => {c_}
   */
  const auto& projection_node_a = ProjectionNode::Make(ExpressionVector_(a_, b_, c_), mock_node1_);
  const auto& aggregate_node = AggregateNode::Make(ExpressionVector_(a_, b_), ExpressionVector_(Any_(c_)), mock_node1_);
  const auto& projection_node_b = ProjectionNode::Make(ExpressionVector_(a_, b_, c_), aggregate_node);

  const auto& union_all_node = UnionNode::Make(SetOperationMode::kAll);
  union_all_node->SetLeftInput(projection_node_a);
  union_all_node->SetRightInput(projection_node_b);

  // Prerequisite: Input nodes have differing FDs
  const auto& expected_fd_a_b = FunctionalDependency({a_, b_}, {c_});
  EXPECT_EQ(projection_node_a->FunctionalDependencies().size(), 1);
  EXPECT_EQ(projection_node_a->FunctionalDependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(projection_node_b->FunctionalDependencies().size(), 2);
  EXPECT_EQ(projection_node_b->FunctionalDependencies().at(0), non_trivial_fd_b);
  EXPECT_EQ(projection_node_b->FunctionalDependencies().at(1), expected_fd_a_b);

  // Test: We expect both input FD-sets to be intersected. Therefore, only one FD should survive.
  EXPECT_EQ(union_all_node->FunctionalDependencies().size(), 1);
  EXPECT_EQ(union_all_node->FunctionalDependencies().at(0), non_trivial_fd_b);
}

}  // namespace skyrise
