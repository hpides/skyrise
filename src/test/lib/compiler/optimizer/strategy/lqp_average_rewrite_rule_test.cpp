#include "compiler/optimizer/strategy/lqp_average_rewrite_rule.hpp"

#include <string>
#include <vector>

#include "compiler/logical_query_plan/aggregate_node.hpp"
#include "compiler/logical_query_plan/alias_node.hpp"
#include "compiler/logical_query_plan/join_node.hpp"
#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "compiler/optimizer/strategy/strategy_base_test.hpp"
#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "metadata/mock_catalog.hpp"
#include "testing/testing_assert.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class LqpAverageRewriteRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    rule_ = std::make_shared<LqpAverageRewriteRule>();

    // MockNode does not support nullable columns, which are relevant for some tests. Therefore, a StoredTableNode is
    // used.
    const auto mock_catalog = std::make_shared<MockCatalog>();
    const auto column_definitions = TableColumnDefinitions{{"a", DataType::kInt, false}, {"b", DataType::kInt, true}};
    mock_catalog->AddTableSchema("table_a", TableSchema::FromTableColumnDefinitions(column_definitions));

    stored_table_node_ = StoredTableNode::Make("table_a", mock_catalog);
    a_ = LqpColumn_(stored_table_node_, ColumnId{0});
    b_ = LqpColumn_(stored_table_node_, ColumnId{1});
  }

 protected:
  std::shared_ptr<LqpAverageRewriteRule> rule_;
  std::shared_ptr<StoredTableNode> stored_table_node_;
  std::shared_ptr<MockNode> mock_node_;
  std::shared_ptr<LqpColumnExpression> a_;
  std::shared_ptr<LqpColumnExpression> b_;
};

// TEST_F(LqpAverageRewriteRuleTest, SingleAverage) {
//  // clang-format off
//  const auto input_lqp =
//  AggregateNode::Make(ExpressionVector_(a_, b_, c_), ExpressionVector_(Avg_(d_)),
//    stored_table_node__);
//
//  const auto expected_lqp =
//  ProjectionNode::Make(ExpressionVector_(a_, b_, c_, Div_(Sum_(d_), Count(d_))),
//    AggregateNode::Make(ExpressionVector_(a_, b_, c_), ExpressionVector_(Sum_(d_), Count_()),
//      stored_table_node_);
//  // clang-format on
//
//  const auto actual_lqp = rule_->ApplyTo(input_lqp);
//
//  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
//}
//
// TEST_F(LqpAverageRewriteRuleTest, MultipleAverages) {
//  // clang-format off
//  const auto input_lqp =
//  AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(Avg_(b_), Sum(c_), Avg_(d_)),
//    stored_table_node_);
//
//  const auto expected_lqp =
//  ProjectionNode::Make(ExpressionVector_(a_, Div_(Sum_(d_), Count(d_))),
//    AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(Sum_(b_), Sum_(c_), Sum_(d_), Count_(d_)),
//      stored_table_node_);
//  // clang-format on
//
//  const auto actual_lqp = rule_->ApplyTo(input_lqp);
//
//  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
//}

TEST_F(LqpAverageRewriteRuleTest, AddSumAndCount) {
  // SELECT AVG(b) -> SUM(b) / COUNT(b) AS AVG(b)
  // Requires COUNT(b) to be added to the aggregates because b is nullable.
  // clang-format off
  const auto input_lqp =
  AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(Avg_(b_)),  // NOLINT
    stored_table_node_);

  const auto expected_aliases = std::vector<std::string>{"a", "AVG(b)"};
  const auto projection_expressions = ExpressionVector_(a_, Div_(Cast_(Sum_(b_), DataType::kDouble), Count_(b_)));
  const auto expected_lqp =
  AliasNode::Make(projection_expressions, expected_aliases, // NOLINT
    ProjectionNode::Make(projection_expressions, // NOLINT
      AggregateNode::Make(ExpressionVector_(a_), ExpressionVector_(Sum_(b_), Count_(b_)),
        stored_table_node_)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpAverageRewriteRuleTest, ReuseSumAddCount) {
  // SELECT SUM(b), COUNT(*), AVG(b) -> SUM(b), COUNT(*), SUM(b) / COUNT(b) AS AVG(b)
  // Requires COUNT(b) to be added to the aggregates because b is nullable.
  // clang-format off
  const auto input_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(b_), CountStarLqp_(stored_table_node_), Avg_(b_)),  // NOLINT
    stored_table_node_);

  const auto expected_aliases = std::vector<std::string>{"SUM(b)", "COUNT(*)", "AVG(b)"};
  const auto projection_expressions = ExpressionVector_(Sum_(b_), CountStarLqp_(stored_table_node_), Div_(Cast_(Sum_(b_), DataType::kDouble), Count_(b_))); // NOLINT
  const auto expected_lqp =
  AliasNode::Make(projection_expressions, expected_aliases, // NOLINT
    ProjectionNode::Make(projection_expressions, // NOLINT
      AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(b_), CountStarLqp_(stored_table_node_), Count_(b_)),
        stored_table_node_)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpAverageRewriteRuleTest, ReuseSumAddCountStar) {
  // SELECT SUM(a), COUNT(b), AVG(a) -> SUM(a), COUNT(b), SUM(a) / COUNT(*) AS AVG(a)
  // Requires COUNT(*) or COUNT(a) to be added to the aggregates because COUNT(b) is unrelated. COUNT(*) is preferred
  // because a is non-nullable.
  // clang-format off
  const auto input_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), Count_(b_), Avg_(a_)),
    stored_table_node_);

  const auto expected_aliases = std::vector<std::string>{"SUM(a)", "COUNT(b)", "AVG(a)"};
  const auto projection_expressions = ExpressionVector_(Sum_(a_), Count_(b_), Div_(Cast_(Sum_(a_), DataType::kDouble), CountStarLqp_(stored_table_node_))); // NOLINT
  const auto expected_lqp =
  AliasNode::Make(projection_expressions, expected_aliases,
    ProjectionNode::Make(projection_expressions,
      AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), Count_(b_), CountStarLqp_(stored_table_node_)),
        stored_table_node_)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpAverageRewriteRuleTest, UseExistingSumAndCount) {
  // SELECT SUM(a), COUNT(a), AVG(a) -> SUM(a), COUNT(a), SUM(a) / COUNT(a) AS AVG(a)
  // clang-format off
  const auto input_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), Count_(a_), Avg_(a_)),
    stored_table_node_);

  const auto expected_aliases = std::vector<std::string>{"SUM(a)", "COUNT(a)", "AVG(a)"};
  const auto projection_expressions = ExpressionVector_(Sum_(a_), Count_(a_), Div_(Cast_(Sum_(a_), DataType::kDouble), Count_(a_)));  // NOLINT
  const auto expected_lqp =
  AliasNode::Make(projection_expressions, expected_aliases,
    ProjectionNode::Make(projection_expressions,
      AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), Count_(a_)),
        stored_table_node_)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpAverageRewriteRuleTest, UseExistingSumAndCountStar) {
  // SELECT SUM(a), COUNT(*), AVG(a) -> SUM(a), COUNT(*), SUM(a) / COUNT(*) AS AVG(a) as a is not NULLable
  // clang-format off
  const auto input_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), CountStarLqp_(stored_table_node_), Avg_(a_)),
    stored_table_node_);

  const auto expected_aliases = std::vector<std::string>{"SUM(a)", "COUNT(*)", "AVG(a)"};
  const auto projection_expressions = ExpressionVector_(Sum_(a_), CountStarLqp_(stored_table_node_), Div_(Cast_(Sum_(a_), DataType::kDouble), CountStarLqp_(stored_table_node_))); // NOLINT
  const auto expected_lqp =
  AliasNode::Make(projection_expressions, expected_aliases,
    ProjectionNode::Make(projection_expressions,
      AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), CountStarLqp_(stored_table_node_)),
        stored_table_node_)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpAverageRewriteRuleTest, UseExistingSumAndCountWithGroupBy) {
  // SELECT SUM(a), COUNT(a), AVG(a) GROUP BY b -> SUM(a), COUNT(a), SUM(a) / COUNT(a) AS AVG(a) GROUP BY b
  // clang-format off
  const auto input_lqp =
  AggregateNode::Make(ExpressionVector_(b_), ExpressionVector_(Sum_(a_), Count_(a_), Avg_(a_)),
    stored_table_node_);

  const auto expected_aliases = std::vector<std::string>{"b", "SUM(a)", "COUNT(a)", "AVG(a)"};
  const auto projection_expressions = ExpressionVector_(b_, Sum_(a_), Count_(a_), Div_(Cast_(Sum_(a_), DataType::kDouble), Count_(a_))); // NOLINT
  const auto expected_lqp =
  AliasNode::Make(projection_expressions, expected_aliases,
    ProjectionNode::Make(projection_expressions,
      AggregateNode::Make(ExpressionVector_(b_), ExpressionVector_(Sum_(a_), Count_(a_)),
        stored_table_node_)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpAverageRewriteRuleTest, AverageAsSummand) {
  // SELECT SUM(a), COUNT(a) + 1, AVG(a) + 2 -> SUM(a), COUNT(a) + 1, SUM(a) / COUNT(a) + 2 AS AVG(a) + 2
  // clang-format off
  const auto input_lqp =
  ProjectionNode::Make(ExpressionVector_(Sum_(a_), Add_(Count_(a_), 1), Add_(Avg_(a_), 2)),
    AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), Count_(a_), Avg_(a_)),
      stored_table_node_));

  const auto expected_aliases = std::vector<std::string>{"SUM(a)", "COUNT(a) + 1", "AVG(a) + 2"};
  const auto projection_expressions = ExpressionVector_(Sum_(a_), Add_(Count_(a_), 1), Add_(Div_(Cast_(Sum_(a_), DataType::kDouble), Count_(a_)), 2));  // NOLINT
  const auto expected_lqp =
  AliasNode::Make(projection_expressions, expected_aliases,
    ProjectionNode::Make(projection_expressions,
      ProjectionNode::Make(ExpressionVector_(Sum_(a_), Count_(a_), Div_(Cast_(Sum_(a_), DataType::kDouble), Count_(a_))),  // NOLINT
        AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), Count_(a_)),
          stored_table_node_))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LqpAverageRewriteRuleTest, AverageWithAlias) {
  // SELECT SUM(a), COUNT(a) AS foo, AVG(a) AS bar -> SUM(a), COUNT(a) AS foo, SUM(a) / COUNT(a) AS bar
  const auto aliases = std::vector<std::string>{"SUM(a)", "foo", "bar"};

  // clang-format off
  const auto input_lqp =
  AliasNode::Make(ExpressionVector_(Sum_(a_), Count_(a_), Avg_(a_)), aliases,
    AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), Count_(a_), Avg_(a_)),
      stored_table_node_));

  const auto projection_expressions = ExpressionVector_(Sum_(a_), Count_(a_), Div_(Cast_(Sum_(a_), DataType::kDouble), Count_(a_)));  // NOLINT
  const auto expected_lqp =
  AliasNode::Make(projection_expressions, aliases,
    ProjectionNode::Make(projection_expressions,
      AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Sum_(a_), Count_(a_)),
        stored_table_node_)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::ApplyRule(rule_, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace skyrise
