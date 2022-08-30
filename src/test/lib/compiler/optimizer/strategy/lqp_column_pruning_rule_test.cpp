/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/optimizer/strategy/lqp_column_pruning_rule.hpp"

#include "compiler/logical_query_plan/aggregate_node.hpp"
#include "compiler/logical_query_plan/export_node.hpp"
#include "compiler/logical_query_plan/join_node.hpp"
#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/logical_query_plan/predicate_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/sort_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "compiler/logical_query_plan/union_node.hpp"
#include "compiler/optimizer/strategy/strategy_base_test.hpp"
#include "expression/expression_functional.hpp"
#include "testing/testing_assert.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class LqpColumnPruningRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::Make(
        MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}}, "a");
    node_b = MockNode::Make(
        MockNode::ColumnDefinitions{{DataType::kInt, "u"}, {DataType::kInt, "v"}, {DataType::kInt, "w"}}, "b");

    a = node_a->get_column("a");
    b = node_a->get_column("b");
    c = node_a->get_column("c");
    u = node_b->get_column("u");
    v = node_b->get_column("v");
    w = node_b->get_column("w");

    rule_ = std::make_shared<LqpColumnPruningRule>();
  }

  const std::shared_ptr<MockNode> Pruned(const std::shared_ptr<MockNode> node,
                                         const std::vector<ColumnId>& column_ids) {
    const auto pruned_node = std::static_pointer_cast<MockNode>(node->DeepCopy());
    pruned_node->set_pruned_column_ids(column_ids);
    return pruned_node;
  }

  std::shared_ptr<LqpColumnPruningRule> rule_;  // TODO append _
  std::shared_ptr<MockNode> node_a, node_b;
  std::shared_ptr<LqpColumnExpression> a, b, c, u, v, w;
};

TEST_F(LqpColumnPruningRuleTest, NoUnion) {
  std::shared_ptr<AbstractLqpNode> lqp;

  // clang-format off
  lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Mul_(a, u), 5)),
    PredicateNode::Make(GreaterThan_(5, c),
      JoinNode::Make(JoinMode::kInner, GreaterThan_(v, a),
        node_a,
        SortNode::Make(ExpressionVector_(w), std::vector<SortMode>{SortMode::kAscending},  // NOLINT
          node_b))));

  // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
  lqp = lqp->DeepCopy();

  const auto pruned_node_a = Pruned(node_a, {ColumnId{1}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_c = pruned_node_a->get_column("c");

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Mul_(pruned_a, u), 5)),
    PredicateNode::Make(GreaterThan_(5, pruned_c),
      JoinNode::Make(JoinMode::kInner, GreaterThan_(v, pruned_a),
        pruned_node_a,
        SortNode::Make(ExpressionVector_(w), std::vector<SortMode>{SortMode::kAscending},  // NOLINT
          node_b))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpColumnPruningRuleTest, WithUnion) {
  for (auto union_mode : {SetOperationMode::kAll}) {
    // SCOPED_TRACE(std::string{"union_mode: "} + set_operation_mode_to_string.left.at(union_mode));

    auto lqp = std::shared_ptr<AbstractLqpNode>{};

    // clang-format off
    lqp =
    ProjectionNode::Make(ExpressionVector_(a),
      UnionNode::Make(union_mode,
        PredicateNode::Make(GreaterThan_(a, 5),
          node_a),
        PredicateNode::Make(GreaterThan_(b, 5),
          node_a)));

    // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
    lqp = lqp->DeepCopy();


    const auto pruned_node_a = Pruned(node_a, {ColumnId{2}});
    const auto pruned_a = pruned_node_a->get_column("a");
    const auto pruned_b = pruned_node_a->get_column("b");

    const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

    // Column c is not used anywhere above the union, so it can be pruned at least in the Positions mode
    const auto expected_lqp =
    ProjectionNode::Make(ExpressionVector_(pruned_a),
      UnionNode::Make(union_mode,
        PredicateNode::Make(GreaterThan_(pruned_a, 5),
          pruned_node_a),
        PredicateNode::Make(GreaterThan_(pruned_b, 5),
          pruned_node_a)));
    // clang-format on

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(LqpColumnPruningRuleTest, WithMultipleProjections) {
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  // clang-format off
  lqp =
  ProjectionNode::Make(ExpressionVector_(a),
    PredicateNode::Make(GreaterThan_(Mul_(a, b), 5),
      ProjectionNode::Make(ExpressionVector_(a, b, Mul_(a, b), c),
        PredicateNode::Make(GreaterThan_(Mul_(a, 2), 5),
          ProjectionNode::Make(ExpressionVector_(a, b, Mul_(a, 2), c),
            node_a)))));

  // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
  lqp = lqp->DeepCopy();

  const auto pruned_node_a = Pruned(node_a, {ColumnId{2}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(pruned_a),
    PredicateNode::Make(GreaterThan_(Mul_(pruned_a, pruned_b), 5),
      ProjectionNode::Make(ExpressionVector_(pruned_a, Mul_(pruned_a, pruned_b)),
        PredicateNode::Make(GreaterThan_(Mul_(pruned_a, 2), 5),
          ProjectionNode::Make(ExpressionVector_(pruned_a, pruned_b, Mul_(pruned_a, 2)),
           pruned_node_a)))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpColumnPruningRuleTest, ProjectionDoesNotRecompute) {
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  // clang-format off
  lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Add_(a, 2), 1)),
    PredicateNode::Make(GreaterThan_(Add_(a, 2), 5),
      ProjectionNode::Make(ExpressionVector_(Add_(a, 2)),
        node_a)));

  // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
  lqp = lqp->DeepCopy();

  const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Add_(pruned_a, 2), 1)),
    PredicateNode::Make(GreaterThan_(Add_(pruned_a, 2), 5),
      ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
        pruned_node_a)));
  // clang-format on

  // We can be sure that the top projection node does not recompute a+2 because a is not available

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpColumnPruningRuleTest, Diamond) {
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  // clang-format off
  const auto sub_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(a, 2), Add_(b, 3), Add_(c, 4)),
    node_a);

  lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(a, 2), Add_(b, 3)),
    UnionNode::Make(SetOperationMode::kAll, // changed from kPositions to kAll
      PredicateNode::Make(GreaterThan_(Add_(a, 2), 5),
        sub_lqp),
      PredicateNode::Make(LessThan_(Add_(b, 3), 10),
        sub_lqp)));

  // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
  lqp = lqp->DeepCopy();

  // Column c should be removed even below the UnionNode
  const auto pruned_node_a = Pruned(node_a, {ColumnId{2}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto expected_sub_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2), Add_(pruned_b, 3)),
    pruned_node_a);

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2), Add_(pruned_b, 3)),
    UnionNode::Make(SetOperationMode::kAll,  // changed from kPositions to kAll
      PredicateNode::Make(GreaterThan_(Add_(pruned_a, 2), 5),
        expected_sub_lqp),
      PredicateNode::Make(LessThan_(Add_(pruned_b, 3), 10),
        expected_sub_lqp)));
  // clang-format on

  // We can be sure that the top projection node does not recompute a+2 because a is not available

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpColumnPruningRuleTest, SimpleAggregate) {
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  // clang-format off
  lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(Add_(a, 2))),
    ProjectionNode::Make(ExpressionVector_(a, b, Add_(a, 2)),
      node_a));

  // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
  lqp = lqp->DeepCopy();

  const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(Add_(pruned_a, 2))),
    ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
      pruned_node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpColumnPruningRuleTest, UngroupedCountStar) {
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  // clang-format off
  lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(CountStarLqp_(node_a)),
    ProjectionNode::Make(ExpressionVector_(a, b, Add_(a, 2)),
      node_a));

  // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
  lqp = lqp->DeepCopy();

  const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(CountStarLqp_(pruned_node_a)),
    ProjectionNode::Make(ExpressionVector_(pruned_a),
      pruned_node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpColumnPruningRuleTest, UngroupedCountStarAndSum) {
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  // clang-format off
  lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(CountStarLqp_(node_a), Sum_(b)),
    ProjectionNode::Make(ExpressionVector_(a, b, Add_(a, 2)),
      node_a));

  // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
  lqp = lqp->DeepCopy();

  const auto pruned_node_a = Pruned(node_a, {ColumnId{0}, ColumnId{2}});
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(CountStarLqp_(pruned_node_a), Sum_(pruned_b)),
    ProjectionNode::Make(ExpressionVector_(pruned_b),
      pruned_node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpColumnPruningRuleTest, GroupedCountStar) {
  auto lqp = std::shared_ptr<AbstractLqpNode>{};

  // clang-format off
  lqp =
  AggregateNode::Make(ExpressionVector_(b, a), ExpressionVector_(CountStarLqp_(node_a)),
    ProjectionNode::Make(ExpressionVector_(a, b, Add_(a, 2)),
      node_a));

  // Create deep copy so we can set pruned ColumnIds on node_a below without manipulating the input LQP
  lqp = lqp->DeepCopy();

  const auto pruned_node_a = Pruned(node_a, {ColumnId{2}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);

  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(pruned_b, pruned_a), ExpressionVector_(CountStarLqp_(pruned_node_a)),
    ProjectionNode::Make(ExpressionVector_(pruned_a, pruned_b),
      pruned_node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// TEST_F(LqpColumnPruningRuleTest, InnerJoinToSemiJoin) {
//   {
//     TableColumnDefinitions column_definitions;
//     column_definitions.emplace_back("column0", DataType::kInt, false);
//     auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
//
//     auto& sm = Hyrise::get().storage_manager;
//     sm.add_table("table", table);
//
//     table->add_soft_key_constraint({{ColumnId{0}}, KeyConstraintType::UNIQUE});
//   }
//
//   const auto stored_table_node = StoredTableNode::Make("table");
//   const auto column0 = stored_table_node->get_column("column0");
//
//   // clang-format off
//   const auto lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(a, 2)),
//     JoinNode::Make(JoinMode::kInner, equals_(a, column0),
//       ProjectionNode::Make(ExpressionVector_(a, Add_(b, 1)),
//         node_a),
//       stored_table_node));
//
//   const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
//   const auto pruned_a = pruned_node_a->get_column("a");
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//
//   const auto expected_lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
//     JoinNode::Make(JoinMode::kSemi, equals_(pruned_a, column0),
//       ProjectionNode::Make(ExpressionVector_(pruned_a),
//         pruned_node_a),
//       stored_table_node));
//   // clang-format on
//
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
//}

// TEST_F(LqpColumnPruningRuleTest, MultiPredicateInnerJoinToSemiJoinWithSingleEqui) {
//   // Same as InnerJoinToSemiJoin, but with an additional join predicate that should not change the result.
//
//   {
//     TableColumnDefinitions column_definitions;
//     column_definitions.emplace_back("column0", DataType::kInt, false);
//     column_definitions.emplace_back("column1", DataType::kInt, false);
//     auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
//
//     auto& sm = Hyrise::get().storage_manager;
//     sm.add_table("table", table);
//
//     table->add_soft_key_constraint({{ColumnId{0}}, KeyConstraintType::UNIQUE});
//   }
//
//   const auto stored_table_node = StoredTableNode::Make("table");
//   const auto column0 = stored_table_node->get_column("column0");
//   const auto column1 = stored_table_node->get_column("column1");
//
//   // clang-format off
//   const auto lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(a, 2)),
//     JoinNode::Make(JoinMode::kInner, ExpressionVector_(equals_(a, column0), not_equals_(a, column1)),
//       ProjectionNode::Make(ExpressionVector_(a, Add_(b, 1)),
//         node_a),
//       stored_table_node));
//
//   const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
//   const auto pruned_a = pruned_node_a->get_column("a");
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//
//   const auto expected_lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
//     JoinNode::Make(JoinMode::kSemi, ExpressionVector_(equals_(pruned_a, column0), not_equals_(pruned_a, column1)),
//       ProjectionNode::Make(ExpressionVector_(pruned_a),
//         pruned_node_a),
//       stored_table_node));
//   // clang-format on
//
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }

// TEST_F(LqpColumnPruningRuleTest, MultiPredicateInnerJoinToSemiJoinWithMultiEqui) {
//   /**
//    * Defines a multi-column key constraint (column0, column1) and two inner join predicates of type Equals covering
//    * those two columns. We expect to see a semi join reformulation because the resulting unique constraint matches
//    * the inner join's predicate expressions.
//    */
//   {
//     TableColumnDefinitions column_definitions;
//     column_definitions.emplace_back("column0", DataType::kInt, false);
//     column_definitions.emplace_back("column1", DataType::kInt, false);
//     auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
//
//     auto& sm = Hyrise::get().storage_manager;
//     sm.add_table("table", table);
//
//     table->add_soft_key_constraint({{ColumnId{0}, ColumnId{1}}, KeyConstraintType::UNIQUE});
//   }
//
//   const auto stored_table_node = StoredTableNode::Make("table");
//   const auto column0 = stored_table_node->get_column("column0");
//   const auto column1 = stored_table_node->get_column("column1");
//
//   // clang-format off
//   const auto lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(a, 2)),
//     JoinNode::Make(JoinMode::kInner, ExpressionVector_(equals_(a, column0), equals_(a, column1)),
//       ProjectionNode::Make(ExpressionVector_(a, Add_(b, 1)),
//         node_a),
//     stored_table_node));
//
//   const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
//   const auto pruned_a = pruned_node_a->get_column("a");
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//
//   const auto expected_lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
//     JoinNode::Make(JoinMode::kSemi, ExpressionVector_(equals_(pruned_a, column0), equals_(pruned_a, column1)),
//       ProjectionNode::Make(ExpressionVector_(pruned_a),
//         pruned_node_a),
//       stored_table_node));
//   // clang-format on
//
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }

// TEST_F(LqpColumnPruningRuleTest, DoNotTouchInnerJoinWithNonEqui) {
//   {
//     TableColumnDefinitions column_definitions;
//     column_definitions.emplace_back("column0", DataType::kInt, false);
//     auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
//
//     auto& sm = Hyrise::get().storage_manager;
//     sm.add_table("table", table);
//
//     table->add_soft_key_constraint({{ColumnId{0}}, KeyConstraintType::UNIQUE});
//   }
//
//   const auto stored_table_node = StoredTableNode::Make("table");
//   const auto column0 = stored_table_node->get_column("column0");
//
//   // clang-format off
//   const auto lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(a, 2)),
//     JoinNode::Make(JoinMode::kInner, GreaterThan_(a, column0),
//       ProjectionNode::Make(ExpressionVector_(a, Add_(b, 1)),
//         node_a),
//       stored_table_node));
//
//   const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
//   const auto pruned_a = pruned_node_a->get_column("a");
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//
//   // Still expect it to prune b+1
//   const auto expected_lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
//     JoinNode::Make(JoinMode::kInner, GreaterThan_(pruned_a, column0),
//       ProjectionNode::Make(ExpressionVector_(pruned_a),
//         pruned_node_a),
//       stored_table_node));
//   // clang-format on
//
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }
//
// TEST_F(LqpColumnPruningRuleTest, DoNotTouchInnerJoinWithoutUniqueConstraint) {
//   // Based on the InnerJoinToSemiJoin test.
//   {
//     TableColumnDefinitions column_definitions;
//     column_definitions.emplace_back("column0", DataType::kInt, false);
//     auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
//
//     auto& sm = Hyrise::get().storage_manager;
//     sm.add_table("table", table);
//   }
//
//   const auto stored_table_node = StoredTableNode::Make("table");
//   const auto column0 = stored_table_node->get_column("column0");
//
//   // clang-format off
//   const auto lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(a, 2)),
//     JoinNode::Make(JoinMode::kInner, equals_(a, column0),
//       ProjectionNode::Make(ExpressionVector_(a, Add_(b, 1)),
//         node_a),
//       stored_table_node));
//
//   const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
//   const auto pruned_a = pruned_node_a->get_column("a");
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//
//   const auto expected_lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
//     JoinNode::Make(JoinMode::kInner, equals_(pruned_a, column0),
//       ProjectionNode::Make(ExpressionVector_(pruned_a),
//         pruned_node_a),
//       stored_table_node));
//   // clang-format on
//
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }
//
// TEST_F(LqpColumnPruningRuleTest, DoNotTouchInnerJoinWithoutMatchingUniqueConstraint) {
//   /**
//    * Based on the InnerJoinToSemiJoin test.
//    *
//    * We define a multi-column key constraint (column0, column1), but only a single Equals-predicate for the inner
//    * join (a == column0). Hence, the resulting unique constraint does not match the expressions of the
//    * single Equals-predicate and we should not see a semi join reformulation.
//    */
//
//   {
//     TableColumnDefinitions column_definitions;
//     column_definitions.emplace_back("column0", DataType::kInt, false);
//     column_definitions.emplace_back("column1", DataType::kInt, false);
//     auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
//
//     auto& sm = Hyrise::get().storage_manager;
//     sm.add_table("table", table);
//
//     table->add_soft_key_constraint({{ColumnId{0}, ColumnId{1}}, KeyConstraintType::UNIQUE});
//   }
//
//   const auto stored_table_node = StoredTableNode::Make("table");
//   const auto column0 = stored_table_node->get_column("column0");
//
//   // clang-format off
//   const auto lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(a, 2)),
//     JoinNode::Make(JoinMode::kInner, equals_(a, column0),
//       ProjectionNode::Make(ExpressionVector_(a, Add_(b, 1)),
//         node_a),
//       stored_table_node));
//
//   const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
//   const auto pruned_a = pruned_node_a->get_column("a");
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//
//   const auto expected_lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
//     JoinNode::Make(JoinMode::kInner, equals_(pruned_a, column0),
//       ProjectionNode::Make(ExpressionVector_(pruned_a),
//         pruned_node_a),
//       stored_table_node));
//   // clang-format on
//
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }
//
// TEST_F(LqpColumnPruningRuleTest, DoNotTouchNonInnerJoin) {
//   // Based on the InnerJoinToSemiJoin test.
//   {
//     TableColumnDefinitions column_definitions;
//     column_definitions.emplace_back("column0", DataType::kInt, false);
//     auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
//
//     auto& sm = Hyrise::get().storage_manager;
//     sm.add_table("table", table);
//
//     table->add_soft_key_constraint({{ColumnId{0}}, KeyConstraintType::PRIMARY_KEY});
//   }
//
//   const auto stored_table_node = StoredTableNode::Make("table");
//   const auto column0 = stored_table_node->get_column("column0");
//
//   // clang-format off
//   const auto lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(a, 2)),
//     JoinNode::Make(JoinMode::kLeft, equals_(a, column0),
//       ProjectionNode::Make(ExpressionVector_(a, Add_(b, 1)),
//         node_a),
//       stored_table_node));
//
//   const auto pruned_node_a = Pruned(node_a, {ColumnId{1}, ColumnId{2}});
//   const auto pruned_a = pruned_node_a->get_column("a");
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//
//   const auto expected_lqp =
//   ProjectionNode::Make(ExpressionVector_(Add_(pruned_a, 2)),
//     JoinNode::Make(JoinMode::kLeft, equals_(pruned_a, column0),
//       ProjectionNode::Make(ExpressionVector_(pruned_a),
//         pruned_node_a),
//       stored_table_node));
//   // clang-format on
//
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }

// TEST_F(LqpColumnPruningRuleTest, DoNotPruneUpdateInputs) {
//   // Do not prune away input columns to Update, Update needs them all
//
//   // clang-format off
//   const auto select_rows_lqp =
//   PredicateNode::Make(GreaterThan_(a, 5),
//     node_a);
//
//   const auto lqp =
//   UpdateNode::Make("dummy",
//     select_rows_lqp,
//     ProjectionNode::Make(ExpressionVector_(a, Add_(b, 1), c),
//       select_rows_lqp));
//   // clang-format on
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//   const auto expected_lqp = lqp->DeepCopy();
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }
//
// TEST_F(LqpColumnPruningRuleTest, DoNotPruneInsertInputs) {
//   // Do not prune away input columns to Insert, Insert needs them all
//
//   // clang-format off
//   const auto lqp =
//   InsertNode::Make("dummy",
//     PredicateNode::Make(GreaterThan_(a, 5),
//       node_a));
//   // clang-format on
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//   const auto expected_lqp = lqp->DeepCopy();
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }
//
// TEST_F(LqpColumnPruningRuleTest, DoNotPruneDeleteInputs) {
//   // Do not prune away input columns to Delete, Delete needs them all
//
//   // clang-format off
//   const auto lqp =
//   DeleteNode::Make(
//     PredicateNode::Make(GreaterThan_(a, 5),
//       node_a));
//   // clang-format on
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//   const auto expected_lqp = lqp->DeepCopy();
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }
//
// TEST_F(LqpColumnPruningRuleTest, DoNotPruneExportInputs) {
//   // Do not prune away input columns to Export, Export needs them all
//
//   // clang-format off
//   const auto lqp =
//   ExportNode::Make("dummy", "dummy.csv", FileType::Auto,
//     PredicateNode::Make(GreaterThan_(a, 5),
//       node_a));
//   // clang-format on
//
//   const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, lqp);
//   const auto expected_lqp = lqp->DeepCopy();
//   EXPECT_LQP_EQ(actual_lqp, expected_lqp);
// }

}  // namespace skyrise
