/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/lqp_utils.hpp"

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "compiler/logical_query_plan/dummy_table_node.hpp"
#include "compiler/logical_query_plan/join_node.hpp"
#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/logical_query_plan/predicate_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/union_node.hpp"
#include "expression/expression_functional.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class LqpUtilsTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "a"}, {DataType::kInt, "b"}}, "node_a");
    node_b = MockNode::Make(MockNode::ColumnDefinitions{{DataType::kInt, "x"}, {DataType::kInt, "y"}}, "node_b");

    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    b_x = node_b->get_column("x");
    b_y = node_b->get_column("y");
  }

  std::shared_ptr<MockNode> node_a, node_b;
  std::shared_ptr<LqpColumnExpression> a_a, a_b, b_x, b_y;
};

TEST_F(LqpUtilsTest, VisitLqp) {
  // clang-format off
   const auto expected_nodes = std::vector<std::shared_ptr<AbstractLqpNode>>{
     PredicateNode::Make(GreaterThan_(a_a, 4)), UnionNode::Make(SetOperationMode::kAll),
     PredicateNode::Make(LessThan_(a_a, 4)), PredicateNode::Make(Equals_(a_a, 4)), node_a};
  // clang-format on

  expected_nodes[0]->SetLeftInput(expected_nodes[1]);
  expected_nodes[1]->SetLeftInput(expected_nodes[2]);
  expected_nodes[1]->SetRightInput(expected_nodes[3]);
  expected_nodes[2]->SetLeftInput(node_a);
  expected_nodes[3]->SetLeftInput(node_a);

  {
    // Visit AbstractLqpNode
    auto actual_nodes = std::vector<std::shared_ptr<AbstractLqpNode>>{};
    VisitLqp(expected_nodes[0], [&](const auto& node) {
      actual_nodes.emplace_back(node);
      return LqpVisitation::kVisitInputs;
    });

    EXPECT_EQ(actual_nodes, expected_nodes);
  }

  {
    // Visit PredicateNode
    auto actual_nodes = std::vector<std::shared_ptr<AbstractLqpNode>>{};
    VisitLqp(std::static_pointer_cast<PredicateNode>(expected_nodes[0]), [&](const auto& node) {
      actual_nodes.emplace_back(node);
      return LqpVisitation::kVisitInputs;
    });

    EXPECT_EQ(actual_nodes, expected_nodes);
  }
}

TEST_F(LqpUtilsTest, VisitLqpUpwards) {
  // clang-format off
   const auto expected_nodes = std::vector<std::shared_ptr<AbstractLqpNode>>{node_a,
     PredicateNode::Make(GreaterThan_(a_a, 4)), PredicateNode::Make(LessThan_(a_a, 4)),
     UnionNode::Make(SetOperationMode::kAll), PredicateNode::Make(Equals_(a_a, 4))};
  // clang-format on

  expected_nodes[4]->SetLeftInput(expected_nodes[3]);
  expected_nodes[3]->SetLeftInput(expected_nodes[1]);
  expected_nodes[3]->SetRightInput(expected_nodes[2]);
  expected_nodes[1]->SetLeftInput(node_a);
  expected_nodes[2]->SetLeftInput(node_a);

  {
    auto actual_nodes = std::vector<std::shared_ptr<AbstractLqpNode>>{};
    VisitLqpUpwards(node_a, [&](const auto& node) {
      actual_nodes.emplace_back(node);
      return LqpUpwardVisitation::kVisitOutputs;
    });

    EXPECT_EQ(actual_nodes, expected_nodes);
  }
}

TEST_F(LqpUtilsTest, LqpFindNodesByType) {
  auto dummy_table_node = DummyTableNode::Make();
  auto literal = Add_(Value_(1), Value_(2));
  // clang-format off
  auto lqp =
  JoinNode::Make(JoinMode::kSemi, Equals_(b_y, literal),
    JoinNode::Make(JoinMode::kInner, Equals_(a_a, b_x),
      UnionNode::Make(SetOperationMode::kAll,
        PredicateNode::Make(GreaterThan_(a_a, 700),
          node_a),
        PredicateNode::Make(LessThan_(a_b, 123),
          node_a)),
      node_b),
    ProjectionNode::Make(ExpressionVector_(literal),
      dummy_table_node));

  // We do not expect duplicate nodes in the output
  const auto mock_nodes = LqpFindNodesByType(lqp, LqpNodeType::kMock);
  ASSERT_EQ(mock_nodes.size(), 2);
  EXPECT_EQ(mock_nodes.at(0), node_b);
  EXPECT_EQ(mock_nodes.at(1), node_a);

  const auto dummy_table_nodes = LqpFindNodesByType(lqp, LqpNodeType::kDummyTable);
  ASSERT_EQ(dummy_table_nodes.size(), 1);
  EXPECT_EQ(dummy_table_nodes.at(0), dummy_table_node);

  const auto predicate_nodes = LqpFindNodesByType(lqp, LqpNodeType::kPredicate);
  ASSERT_EQ(predicate_nodes.size(), 2);

  const auto stored_table_nodes = LqpFindNodesByType(lqp, LqpNodeType::kStoredTable);
  EXPECT_TRUE(stored_table_nodes.empty());
}

TEST_F(LqpUtilsTest, LqpFindLeaves) {
  // Based on LqpFindNodesByType test
  auto dummy_table_node = DummyTableNode::Make();
  auto literal = Add_(Value_(1), Value_(2));
  // clang-format off
  auto lqp =
  JoinNode::Make(JoinMode::kSemi, Equals_(b_y, literal),
    JoinNode::Make(JoinMode::kInner, Equals_(a_a, b_x),
      UnionNode::Make(SetOperationMode::kAll,
        PredicateNode::Make(GreaterThan_(a_a, 700),
          node_a),
        PredicateNode::Make(LessThan_(a_b, 123),
          node_a)),
      node_b),
    ProjectionNode::Make(ExpressionVector_(literal),
      dummy_table_node));
  // clang-format on

  const auto leaf_nodes = lqp_find_leaves(lqp);
  ASSERT_EQ(leaf_nodes.size(), 3);
  EXPECT_EQ(leaf_nodes.at(0), node_b);
  EXPECT_EQ(leaf_nodes.at(1), dummy_table_node);
  EXPECT_EQ(leaf_nodes.at(2), node_a);
}

}  // namespace skyrise
