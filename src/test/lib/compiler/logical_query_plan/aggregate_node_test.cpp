/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/aggregate_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/mock_node.hpp"
#include "constraint_test_utils.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "types.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class AggregateNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    mock_node_ = MockNode::Make(
        MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}}, "t_a");

    a_ = mock_node_->get_column("a");
    b_ = mock_node_->get_column("b");
    c_ = mock_node_->get_column("c");

    // SELECT a, c, SUM(a+b), SUM(a+c) AS some_sum [...] GROUP BY a, c
    // Columns are ordered as specified in the SELECT list
    group_by_expressions_ = ExpressionVector_(a_, c_);
    aggregate_expressions_ = ExpressionVector_(Sum_(Add_(a_, b_)), Sum_(Add_(a_, c_)));
    aggregate_node_ = AggregateNode::Make(group_by_expressions_, aggregate_expressions_, mock_node_);
  }

  std::shared_ptr<MockNode> mock_node_;
  std::shared_ptr<AggregateNode> aggregate_node_;
  std::vector<std::shared_ptr<AbstractExpression>> group_by_expressions_, aggregate_expressions_;
  std::shared_ptr<LqpColumnExpression> a_, b_, c_;
};

TEST_F(AggregateNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(aggregate_node_->OutputExpressions().size(), 4u);
  EXPECT_EQ(*aggregate_node_->OutputExpressions().at(0), *a_);
  EXPECT_EQ(*aggregate_node_->OutputExpressions().at(1), *c_);
  EXPECT_EQ(*aggregate_node_->OutputExpressions().at(2), *Sum_(Add_(a_, b_)));
  EXPECT_EQ(*aggregate_node_->OutputExpressions().at(3), *Sum_(Add_(a_, c_)));
}

TEST_F(AggregateNodeTest, NodeExpressions) {
  ASSERT_EQ(aggregate_node_->node_expressions_.size(), 4u);
  EXPECT_EQ(*aggregate_node_->node_expressions_.at(0), *a_);
  EXPECT_EQ(*aggregate_node_->node_expressions_.at(1), *c_);
  EXPECT_EQ(*aggregate_node_->node_expressions_.at(2), *Sum_(Add_(a_, b_)));
  EXPECT_EQ(*aggregate_node_->node_expressions_.at(3), *Sum_(Add_(a_, c_)));
}

TEST_F(AggregateNodeTest, Description) {
  auto description = aggregate_node_->Description();

  EXPECT_EQ(description, "[Aggregate] GroupBy: [a, c] Aggregates: [SUM(a + b), SUM(a + c)]");
}

TEST_F(AggregateNodeTest, HashingAndEqualityCheck) {
  const auto same_aggregate_node = AggregateNode::Make(
      ExpressionVector_(a_, c_), ExpressionVector_(Sum_(Add_(a_, b_)), Sum_(Add_(a_, c_))), mock_node_);

  EXPECT_EQ(*aggregate_node_, *same_aggregate_node);
  EXPECT_EQ(*same_aggregate_node, *aggregate_node_);
  EXPECT_EQ(*aggregate_node_, *aggregate_node_);

  // Build slightly different AggregateNodes
  const auto different_aggregate_node_a =
      AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(Sum_(Add_(a_, b_)), Sum_(Add_(a_, c_))), mock_node_);
  const auto different_aggregate_node_b = AggregateNode::Make(
      ExpressionVector_(a_, c_), ExpressionVector_(Sum_(Add_(a_, 2)), Sum_(Add_(a_, c_))), mock_node_);
  const auto different_aggregate_node_c = AggregateNode::Make(
      ExpressionVector_(a_, c_), ExpressionVector_(Sum_(Add_(a_, b_)), Sum_(Add_(a_, c_)), Min_(a_)), mock_node_);
  const auto different_aggregate_node_d = AggregateNode::Make(
      ExpressionVector_(a_, a_), ExpressionVector_(Sum_(Add_(a_, b_)), Sum_(Add_(a_, c_))), mock_node_);

  EXPECT_NE(*aggregate_node_, *different_aggregate_node_a);
  EXPECT_NE(*aggregate_node_, *different_aggregate_node_b);
  EXPECT_NE(*aggregate_node_, *different_aggregate_node_c);
  EXPECT_NE(*aggregate_node_, *different_aggregate_node_d);

  EXPECT_NE(aggregate_node_->Hash(), different_aggregate_node_a->Hash());
  // aggregate_node_ and different_aggregate_node_b are known to conflict because we do not recurse deep enough to
  // identify the difference in the aggregate expressions. That is acceptable, as long as the comparison identifies
  // the two nodes as non-equal.
  EXPECT_NE(aggregate_node_->Hash(), different_aggregate_node_c->Hash());
  EXPECT_NE(aggregate_node_->Hash(), different_aggregate_node_d->Hash());
}

TEST_F(AggregateNodeTest, Copy) {
  const auto same_aggregate_node = AggregateNode::Make(
      ExpressionVector_(a_, c_), ExpressionVector_(Sum_(Add_(a_, b_)), Sum_(Add_(a_, c_))), mock_node_);
  EXPECT_EQ(*aggregate_node_->DeepCopy(), *same_aggregate_node);
}

TEST_F(AggregateNodeTest, UniqueConstraintsAdd) {
  EXPECT_TRUE(mock_node_->UniqueConstraints()->empty());

  const auto aggregate1 = Sum_(Add_(a_, b_));
  const auto aggregate2 = Sum_(Add_(a_, c_));
  const auto agg_node_a =
      AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(aggregate1, aggregate2), mock_node_);
  const auto agg_node_b =
      AggregateNode::Make(ExpressionVector_(a_, b_), ExpressionVector_(aggregate1, aggregate2), mock_node_);

  // Check whether AggregateNode adds a new unique constraint for its group-by column(s)
  {
    EXPECT_EQ(agg_node_a->UniqueConstraints()->size(), 1);
    const auto unique_constraint = *agg_node_a->UniqueConstraints()->cbegin();
    EXPECT_EQ(unique_constraint.expressions.size(), 1);
    // TODO(anyone): C++20: Replace with .contains
    EXPECT_TRUE(unique_constraint.expressions.find(a_) != unique_constraint.expressions.end());
  }
  {
    EXPECT_EQ(agg_node_b->UniqueConstraints()->size(), 1);
    const auto unique_constraint = *agg_node_b->UniqueConstraints()->cbegin();
    EXPECT_EQ(unique_constraint.expressions.size(), 2);
    // TODO(anyone): C++20: Replace with .contains
    EXPECT_TRUE(unique_constraint.expressions.find(a_) != unique_constraint.expressions.end());
    EXPECT_TRUE(unique_constraint.expressions.find(b_) != unique_constraint.expressions.end());
  }
}

TEST_F(AggregateNodeTest, UniqueConstraintsForwardingSimple) {
  const auto key_constraint_b = TableKeyConstraint{{b_->original_column_id_}, KeyConstraintType::kUnique};
  const auto key_constraint_c = TableKeyConstraint{{c_->original_column_id_}, KeyConstraintType::kUnique};
  mock_node_->set_key_constraints({key_constraint_b, key_constraint_c});
  EXPECT_EQ(mock_node_->UniqueConstraints()->size(), 2);

  const auto aggregate_c = Sum_(c_);
  aggregate_node_ = AggregateNode::Make(ExpressionVector_(a_, b_), ExpressionVector_(aggregate_c), mock_node_);
  const auto& unique_constraints = aggregate_node_->UniqueConstraints();

  /**
   * Expected behaviour:
   *  - key_constraint_b remains valid since b_ is part of the group-by columns.
   *  - key_constraint_c, however, should be discarded because c_ gets aggregated.
   */

  // Basic check
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(key_constraint_b, unique_constraints));
}

TEST_F(AggregateNodeTest, UniqueConstraintsForwardingAnyAggregates) {
  const auto key_constraint_b = TableKeyConstraint{{b_->original_column_id_}, KeyConstraintType::kUnique};
  const auto key_constraint_c = TableKeyConstraint{{c_->original_column_id_}, KeyConstraintType::kUnique};
  mock_node_->set_key_constraints({key_constraint_b, key_constraint_c});
  EXPECT_EQ(mock_node_->UniqueConstraints()->size(), 2);

  const auto aggregate_b = Any_(b_);
  const auto aggregate_c = Sum_(c_);
  aggregate_node_ = AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(aggregate_b, aggregate_c), mock_node_);
  const auto& unique_constraints = aggregate_node_->UniqueConstraints();

  /**
   * Expected behaviour:
   *  - key_constraint_b remains valid because b_ is aggregated via ANY(), a pseudo aggregate function used
   *    by the DependentGroupByReductionRule to optimize group-bys.
   *  - key_constraint_c should be discarded because c_ is aggregated.
   *  - Also, we should gain a new unique constraint, covering all group-by columns.
   */

  // Basic check
  EXPECT_EQ(unique_constraints->size(), 2);
  // In-depth check
  EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(key_constraint_b, unique_constraints));
  const auto key_constraint_group_by = TableKeyConstraint{{a_->original_column_id_}, KeyConstraintType::kUnique};
  EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(key_constraint_group_by, unique_constraints));
}

TEST_F(AggregateNodeTest, UniqueConstraintsNoDuplicates) {
  // Prepare single unique constraint
  const auto table_key_constraint = TableKeyConstraint{{a_->original_column_id_}, KeyConstraintType::kUnique};
  mock_node_->set_key_constraints({table_key_constraint});
  EXPECT_EQ(mock_node_->UniqueConstraints()->size(), 1);

  const auto aggregate1 = Sum_(b_);
  const auto aggregate2 = Sum_(c_);
  aggregate_node_ = AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(aggregate1, aggregate2), mock_node_);

  /**
   * AggregateNode should try to create a new unique constraint from its group-by-column a_. It is the same as
   * MockNode's unique constraint which gets forwarded.
   *
   * Expected behaviour: AggregateNode should not output the same unique constraint twice.
   */

  // Basic check
  const auto& unique_constraints = aggregate_node_->UniqueConstraints();
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(table_key_constraint, unique_constraints));
}

TEST_F(AggregateNodeTest, UniqueConstraintsNoSupersets) {
  // Prepare single unique constraint
  const auto table_key_constraint = TableKeyConstraint{{a_->original_column_id_}, KeyConstraintType::kUnique};
  mock_node_->set_key_constraints({table_key_constraint});
  EXPECT_EQ(mock_node_->UniqueConstraints()->size(), 1);

  const auto aggregate = Sum_(c_);
  aggregate_node_ = AggregateNode::Make(ExpressionVector_(a_, b_), ExpressionVector_(aggregate), mock_node_);

  /**
   * AggregateNode should try to create a new unique constraint from both group-by-columns a_ and b_.
   * However, MockNode already has a unique constraint for a_ which gets forwarded. It is shorter and
   * therefore preferred over the unique constraint covering both, a_ and b_.
   *
   * Expected behaviour: AggregateNode should forward the input unique constraint only.
   */

  // Basic check
  const auto& unique_constraints = aggregate_node_->UniqueConstraints();
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(FindUniqueConstraintByKeyConstraint(table_key_constraint, unique_constraints));
}

TEST_F(AggregateNodeTest, FunctionalDependenciesForwarding) {
  // Preparations
  const auto fd_a = FunctionalDependency{{a_}, {c_}};
  const auto fd_b_two_dependent_expressions = FunctionalDependency{{b_}, {a_, c_}};
  mock_node_->set_non_trivial_functional_dependencies({fd_a, fd_b_two_dependent_expressions});
  EXPECT_EQ(mock_node_->FunctionalDependencies().size(), 2);

  const auto aggregate1 = Sum_(Add_(a_, b_));
  const auto aggregate2 = Sum_(Add_(a_, c_));

  // All determinant and dependent expressions are missing.
  const auto& agg_node_a =
      AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(aggregate1, aggregate2), mock_node_);
  EXPECT_TRUE(agg_node_a->NonTrivialFunctionalDependencies().empty());

  // All determinant and dependent expressions are part of the output -> expect FD forwarding
  const auto& agg_node_b =
      AggregateNode::Make(ExpressionVector_(a_, c_), ExpressionVector_(aggregate1, aggregate2), mock_node_);
  EXPECT_EQ(agg_node_b->NonTrivialFunctionalDependencies().size(), 1);
  EXPECT_EQ(agg_node_b->NonTrivialFunctionalDependencies().at(0), fd_a);

  // Special case: All determinant expressions, but only some of the dependent expressions are part of the output
  const auto& agg_node_c =
      AggregateNode::Make(ExpressionVector_(b_, c_), ExpressionVector_(aggregate1, aggregate2), mock_node_);
  const auto expected_fd = FunctionalDependency{{b_}, {c_}};
  EXPECT_EQ(agg_node_c->NonTrivialFunctionalDependencies().size(), 1);
  EXPECT_EQ(agg_node_c->NonTrivialFunctionalDependencies().at(0), expected_fd);
}

TEST_F(AggregateNodeTest, FunctionalDependenciesAdd) {
  // The group-by columns form a new candidate key / unique constraint from which we should derive a trivial FD.
  mock_node_->set_key_constraints({});
  mock_node_->set_non_trivial_functional_dependencies({});

  const auto& fds = aggregate_node_->FunctionalDependencies();
  EXPECT_EQ(fds.size(), 1);
  const auto& fd = fds.at(0);
  const auto expected_determinant_expressions =
      ExpressionUnorderedSet{group_by_expressions_.cbegin(), group_by_expressions_.cend()};
  const auto expected_dependent_expressions =
      ExpressionUnorderedSet{aggregate_expressions_.cbegin(), aggregate_expressions_.cend()};
  EXPECT_EQ(fd.determinant_expressions, expected_determinant_expressions);
  EXPECT_EQ(fd.dependent_expressions, expected_dependent_expressions);
}

}  // namespace skyrise
