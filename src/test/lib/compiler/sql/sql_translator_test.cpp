/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/sql/sql_translator.hpp"

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "compiler/logical_query_plan/aggregate_node.hpp"
#include "compiler/logical_query_plan/alias_node.hpp"
#include "compiler/logical_query_plan/create_view_node.hpp"
#include "compiler/logical_query_plan/drop_view_node.hpp"
#include "compiler/logical_query_plan/dummy_table_node.hpp"
#include "compiler/logical_query_plan/export_node.hpp"
#include "compiler/logical_query_plan/import_node.hpp"
#include "compiler/logical_query_plan/join_node.hpp"
#include "compiler/logical_query_plan/limit_node.hpp"
#include "compiler/logical_query_plan/predicate_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/sort_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "compiler/sql/create_sql_parser_error_message.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "import_export/file_type.hpp"
#include "metadata/mock_catalog.hpp"
#include "testing/testing_assert.hpp"
#include "types.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)
using namespace std::string_literals;            // NOLINT(google-build-using-namespace)

namespace skyrise {

class SqlTranslatorTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    mock_catalog_ = std::make_shared<MockCatalog>();
    mock_catalog_->AddTableSchemaFromFileHeader("int_float", "resources/test_data/tbl/int_float.tbl");
    mock_catalog_->AddTableSchemaFromFileHeader("int_float2", "resources/test_data/tbl/int_float2.tbl");
    mock_catalog_->AddTableSchemaFromFileHeader("int_float5", "resources/test_data/tbl/int_float5.tbl");
    mock_catalog_->AddTableSchemaFromFileHeader("int_int_int", "resources/test_data/tbl/int_int_int.tbl");
    mock_catalog_->AddTableSchemaFromFileHeader("int_string", "resources/test_data/tbl/int_string.tbl");
  }

  void SetUp() override {
    stored_table_node_int_float = StoredTableNode::Make("int_float", mock_catalog_);
    stored_table_node_int_float2 = StoredTableNode::Make("int_float2", mock_catalog_);
    stored_table_node_int_float5 = StoredTableNode::Make("int_float5", mock_catalog_);
    stored_table_node_int_int_int = StoredTableNode::Make("int_int_int", mock_catalog_);
    stored_table_node_int_string = StoredTableNode::Make("int_string", mock_catalog_);

    int_float_a = stored_table_node_int_float->get_column("a");
    int_float_b = stored_table_node_int_float->get_column("b");

    int_string_a = stored_table_node_int_string->get_column("a");
    int_string_b = stored_table_node_int_string->get_column("b");

    int_float2_a = stored_table_node_int_float2->get_column("a");
    int_float2_b = stored_table_node_int_float2->get_column("b");

    int_float5_a = stored_table_node_int_float5->get_column("a");
    int_float5_d = stored_table_node_int_float5->get_column("d");

    int_int_int_a = stored_table_node_int_int_int->get_column("a");
    int_int_int_b = stored_table_node_int_int_int->get_column("b");
    int_int_int_c = stored_table_node_int_int_int->get_column("c");
  }

  static std::pair<std::shared_ptr<skyrise::AbstractLqpNode>, SqlTranslationInfo> SqlToLqpHelper(
      const std::string& query) {
    hsql::SQLParserResult parser_result;
    hsql::SQLParser::parseSQLString(query, &parser_result);
    Assert(parser_result.isValid(), CreateSqlParserErrorMessage(query, parser_result));

    const auto translation_result = SqlTranslator{mock_catalog_}.translate_parser_result(parser_result);
    const auto lqps = translation_result.lqp_nodes;

    Assert(lqps.size() == 1, "Expected just one LQP");
    return {lqps.at(0), translation_result.translation_info};
  }

  static inline std::shared_ptr<MockCatalog> mock_catalog_;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float2;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float5;
  std::shared_ptr<StoredTableNode> stored_table_node_int_int_int;
  std::shared_ptr<StoredTableNode> stored_table_node_int_string;
  std::shared_ptr<LqpColumnExpression> int_float_a, int_float_b;
  std::shared_ptr<LqpColumnExpression> int_float2_a, int_float2_b;
  std::shared_ptr<LqpColumnExpression> int_float5_a, int_float5_d;
  std::shared_ptr<LqpColumnExpression> int_int_int_a, int_int_int_b, int_int_int_c;
  std::shared_ptr<LqpColumnExpression> int_string_a, int_string_b;
};

TEST_F(SqlTranslatorTest, NoFromClause) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT 1 + 2;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Value_(1), Value_(2))),
    DummyTableNode::Make());
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, ExpressionStringTest) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT * FROM int_float WHERE a = 'b'");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::Make(Equals_(int_float_a, std::string{"b"}),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectSingleColumn) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT a FROM int_float;");

  const auto expected_lqp = ProjectionNode::Make(ExpressionVector_(int_float_a), stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectStar) {
  const auto [actual_lqp_no_table, translation_info_no_table] = SqlToLqpHelper("SELECT * FROM int_float;");
  const auto [actual_lqp_table, translation_info] = SqlToLqpHelper("SELECT int_float.* FROM int_float;");

  EXPECT_LQP_EQ(actual_lqp_no_table, stored_table_node_int_float);
  EXPECT_LQP_EQ(actual_lqp_table, stored_table_node_int_float);
}

TEST_F(SqlTranslatorTest, SelectStarSelectsOnlyFromColumns) {
  /**
   * Test that if temporary columns are introduced, these are not selected by "*"
   */

  // "a + b" is a temporary column that shouldn't be in the output
  const auto [actual_lqp_no_table, translation_info_no_table] =
      SqlToLqpHelper("SELECT * FROM int_float ORDER BY a + b");
  const auto [actual_lqp_table, translation_info] = SqlToLqpHelper("SELECT int_float.* FROM int_float ORDER BY a + b");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_b),
    SortNode::Make(ExpressionVector_(Add_(int_float_a, int_float_b)), std::vector<SortMode>{SortMode::kAscending},
      ProjectionNode::Make(ExpressionVector_(Add_(int_float_a, int_float_b), int_float_a, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_no_table, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_table, expected_lqp);
}

TEST_F(SqlTranslatorTest, SimpleArithmeticExpression) {
  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper("SELECT a * b FROM int_float;");
  const auto [actual_lqp_b, translation_info_b] = SqlToLqpHelper("SELECT a / b FROM int_float;");
  const auto [actual_lqp_c, translation_info_c] = SqlToLqpHelper("SELECT a + b FROM int_float;");
  const auto [actual_lqp_d, translation_info_d] = SqlToLqpHelper("SELECT a - b FROM int_float;");
  const auto [actual_lqp_e, translation_info_e] = SqlToLqpHelper("SELECT a % b FROM int_float;");

  // clang-format off
  const auto expected_lqp_a = ProjectionNode::Make(ExpressionVector_(Mul_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_b = ProjectionNode::Make(ExpressionVector_(Div_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_c = ProjectionNode::Make(ExpressionVector_(Add_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_d = ProjectionNode::Make(ExpressionVector_(Sub_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_e = ProjectionNode::Make(ExpressionVector_(Mod_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp_d);
  EXPECT_LQP_EQ(actual_lqp_e, expected_lqp_e);
}

TEST_F(SqlTranslatorTest, NestedArithmeticExpression) {
  // With parentheses: `(a*b) + ((a/b) % 5))`
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT a * b + a / b % 5 FROM int_float;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Mul_(int_float_a, int_float_b), Mod_(Div_(int_float_a, int_float_b), 5))),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAlias) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT a AS column_a, b, b + a AS sum_column FROM int_float;");

  const auto aliases = std::vector<std::string>{{"column_a", "b", "sum_column"}};
  const auto expressions = ExpressionVector_(int_float_a, int_float_b, Add_(int_float_b, int_float_a));

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(expressions, aliases,
    ProjectionNode::Make(expressions, stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasUsedInWhere) {
  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper("SELECT a AS x FROM int_float WHERE a > 5");
  const auto [actual_lqp_b, translation_info_b] = SqlToLqpHelper("SELECT a AS x FROM int_float WHERE x > 5");

  const auto aliases = std::vector<std::string>({"x"});

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a), aliases,
    ProjectionNode::Make(ExpressionVector_(int_float_a),
      PredicateNode::Make(GreaterThan_(int_float_a, Value_(5)),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasUsedInGroupBy) {
  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper("SELECT a AS x FROM int_float GROUP BY a");
  const auto [actual_lqp_b, translation_info_b] = SqlToLqpHelper("SELECT a AS x FROM int_float GROUP BY x");
  const auto [actual_lqp_c, translation_info_c] = SqlToLqpHelper("SELECT a AS x FROM int_float GROUP BY int_float.a");
  const auto [actual_lqp_d, translation_info_d] = SqlToLqpHelper("SELECT a AS x FROM int_float GROUP BY int_float.x");

  const auto aliases = std::vector<std::string>({"x"});

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a), aliases,
    AggregateNode::Make(ExpressionVector_(int_float_a), ExpressionVector_(),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasUsedInGroupByAndHaving) {
  const auto [actual_lqp_a, translation_info_a] =
      SqlToLqpHelper("SELECT a AS x FROM int_float GROUP BY x HAVING a > 5");
  const auto [actual_lqp_b, translation_info_b] =
      SqlToLqpHelper("SELECT a AS x FROM int_float GROUP BY x HAVING x > 5");

  const auto aliases = std::vector<std::string>({"x"});

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a), aliases,
    PredicateNode::Make(GreaterThan_(int_float_a, Value_(5)),
      AggregateNode::Make(ExpressionVector_(int_float_a), ExpressionVector_(),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasUsedInOrderBy) {
  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper("SELECT a AS x, b AS y FROM int_float ORDER BY a, b");
  const auto [actual_lqp_b, translation_info_b] = SqlToLqpHelper("SELECT a AS x, b AS y FROM int_float ORDER BY x, y");

  const auto aliases = std::vector<std::string>({"x", "y"});
  const auto sort_modes = std::vector<SortMode>({SortMode::kAscending, SortMode::kAscending});

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a, int_float_b), aliases,
    SortNode::Make(ExpressionVector_(int_float_a, int_float_b), sort_modes,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasUsedInJoin) {
  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper(
      "SELECT R.a, R.b FROM (SELECT a AS c, b AS d FROM int_float) AS R JOIN int_float2 AS S ON R.b = S.b");
  const auto [actual_lqp_b, translation_info_b] = SqlToLqpHelper(
      "SELECT R.c, R.d FROM (SELECT a AS c, b AS d FROM int_float) AS R JOIN int_float2 AS S ON R.d = S.b");

  const auto aliases = std::vector<std::string>({"c", "d"});

  // clang-format off
  const auto expected_lqp_a =
  ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_b),
    JoinNode::Make(JoinMode::kInner, Equals_(int_float_b, int_float2_b),
      AliasNode::Make(ExpressionVector_(int_float_a, int_float_b), aliases,
        stored_table_node_int_float),
      stored_table_node_int_float2));

  const auto expected_lqp_b =
  AliasNode::Make(ExpressionVector_(int_float_a, int_float_b), aliases,
    expected_lqp_a);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
}

TEST_F(SqlTranslatorTest, SelectListAliasesDifferentForSimilarColumns) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT a AS a1, b AS b2, b AS b3, a AS a3, b AS b1, a AS a2 FROM int_float");

  const auto aliases = std::vector<std::string>({"a1", "b2", "b3", "a3", "b1", "a2"});
  const auto expressions =
      ExpressionVector_(int_float_a, int_float_b, int_float_b, int_float_a, int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(expressions, aliases,
    ProjectionNode::Make(expressions,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasesDifferentForSimilarAggregates) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2, COUNT(*) AS cnt3 FROM int_float");

  const auto aliases = std::vector<std::string>({"cnt1", "cnt2", "cnt3"});
  const auto aggregate = CountStarLqp_(stored_table_node_int_float);
  const auto aggregates = ExpressionVector_(aggregate, aggregate, aggregate);

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(aggregates, aliases,
    ProjectionNode::Make(aggregates,
      AggregateNode::Make(ExpressionVector_(), ExpressionVector_(aggregate),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasesDifferentForSimilarColumnsInSubquery) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "SELECT a1, b2, a3 FROM (SELECT a AS a1, b AS b2, b AS b3, a AS a3, b AS b1, a AS a2 FROM int_float) AS R");

  const auto outer_aliases = std::vector<std::string>({"a1", "b2", "a3"});
  const auto inner_aliases = std::vector<std::string>({"a1", "b2", "b3", "a3", "b1", "a2"});
  const auto outer_expressions = ExpressionVector_(int_float_a, int_float_b, int_float_a);
  const auto inner_expressions =
      ExpressionVector_(int_float_a, int_float_b, int_float_b, int_float_a, int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(outer_expressions, outer_aliases,
    ProjectionNode::Make(outer_expressions,
      AliasNode::Make(inner_expressions, inner_aliases,
        ProjectionNode::Make(inner_expressions,
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasesDifferentForSimilarAggregatesInSubquery) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT * FROM (SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2, COUNT(*) AS cnt3 FROM int_float) AS R");

  const auto aliases = std::vector<std::string>({"cnt1", "cnt2", "cnt3"});
  const auto aggregate = CountStarLqp_(stored_table_node_int_float);
  const auto aggregates = ExpressionVector_(aggregate, aggregate, aggregate);

  // clang-format off
  // Hyrise #1186: Redundant AliasNode due to the SqlTranslator architecture.
  const auto expected_lqp =
  AliasNode::Make(aggregates, aliases,
    AliasNode::Make(aggregates, aliases,
      ProjectionNode::Make(aggregates,
        AggregateNode::Make(ExpressionVector_(), ExpressionVector_(aggregate),
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, RepeatingAggregates) {
  // See Hyrise #1902. Optimally, these queries would produce correct results. Until then, at least make sure they don't
  // produce any wrong ones.

  EXPECT_THROW(SqlToLqpHelper("SELECT COUNT(*) FROM (SELECT COUNT(*) FROM t WHERE a <= 1234) t2"),
               InvalidInputException);

  EXPECT_THROW(SqlToLqpHelper("SELECT COUNT(a) FROM (SELECT a, COUNT(a) FROM t GROUP BY a) t2"), InvalidInputException);

  EXPECT_THROW(SqlToLqpHelper("SELECT COUNT(a) FROM (SELECT a, COUNT(a) AS b FROM t GROUP BY a) t2"),
               InvalidInputException);

  EXPECT_THROW(SqlToLqpHelper("SELECT AVG(a) FROM (SELECT a, AVG(a) FROM t GROUP BY a) t3"), InvalidInputException);
}

TEST_F(SqlTranslatorTest, SelectAggregate) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT COUNT(*) FROM int_float");

  const auto aggregate = CountStarLqp_(stored_table_node_int_float);

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(aggregate),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectAggregates) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT COUNT(*), SUM(a + b) FROM int_float");

  const auto aggregate0 = CountStarLqp_(stored_table_node_int_float);
  const auto aggregate1 = Sum_(Add_(int_float_a, int_float_b));

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(aggregate0, aggregate1),
    ProjectionNode::Make(ExpressionVector_(Add_(int_float_a, int_float_b)),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectAggregatesFromSubqueries) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "SELECT * FROM ("
      "  SELECT COUNT(*) AS cnt1"
      "  FROM int_float"
      ") AS s1, ("
      "  SELECT COUNT(*) AS cnt2"
      "  FROM int_float2"
      ") AS s2");

  const auto aliases = std::vector<std::string>({"cnt1", "cnt2"});
  const auto aggregate0 = CountStarLqp_(stored_table_node_int_float);
  const auto aggregate1 = CountStarLqp_(stored_table_node_int_float2);

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(aggregate0, aggregate1), aliases,
    JoinNode::Make(JoinMode::kCross,
      AliasNode::Make(ExpressionVector_(aggregate0), std::vector<std::string>({"cnt1"}),
        AggregateNode::Make(ExpressionVector_(), ExpressionVector_(aggregate0),
          stored_table_node_int_float)),
      AliasNode::Make(ExpressionVector_(aggregate1), std::vector<std::string>({"cnt2"}),
        AggregateNode::Make(ExpressionVector_(), ExpressionVector_(aggregate1),
          stored_table_node_int_float2))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasesDifferentForSimilarColumnsAndFromColumnAliasing) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT x AS x1, x AS x2, x AS x3, y AS y1, y AS y2, y AS y3 FROM int_float AS R (x, y)");

  const auto aliases = std::vector<std::string>({"x1", "x2", "x3", "y1", "y2", "y3"});
  const auto expressions =
      ExpressionVector_(int_float_a, int_float_a, int_float_a, int_float_b, int_float_b, int_float_b);

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(expressions, aliases,
    ProjectionNode::Make(expressions,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasesDifferentForSimilarColumnsInSubqueryAndFromColumnAliasing) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "SELECT u, z, w FROM (SELECT a AS a1, b AS b2, b AS b3, a AS a3, b AS b1, a AS a2 FROM int_float)"
      "AS R (y, x, v, w, u, z)");

  const auto outer_aliases = std::vector<std::string>({"u", "z", "w"});
  const auto inner_aliases = std::vector<std::string>({"a1", "b2", "b3", "a3", "b1", "a2"});
  const auto outer_expressions = ExpressionVector_(int_float_b, int_float_a, int_float_a);
  const auto inner_expressions =
      ExpressionVector_(int_float_a, int_float_b, int_float_b, int_float_a, int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(outer_expressions, outer_aliases,
    ProjectionNode::Make(outer_expressions,
      AliasNode::Make(inner_expressions, inner_aliases,
        ProjectionNode::Make(inner_expressions,
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasesDifferentForSimilarColumnsUsedInJoin) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "SELECT l.a1, l.a2, r.a1 FROM ("
      "  SELECT a AS a1, a AS a2 FROM int_float"
      ") AS l JOIN ("
      "  SELECT a AS a1, a AS a2 FROM int_float2"
      ") AS r ON l.a1 = r.a2;");

  const auto outer_aliases = std::vector<std::string>({"a1", "a2", "a1"});
  const auto inner_aliases = std::vector<std::string>({"a1", "a2"});
  const auto expressions = ExpressionVector_(int_float_a, int_float_a, int_float2_a);

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(expressions, outer_aliases,
    ProjectionNode::Make(expressions,
      JoinNode::Make(JoinMode::kInner, Equals_(int_float_a, int_float2_a),
        AliasNode::Make(ExpressionVector_(int_float_a, int_float_a), inner_aliases,
          ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_a),
            stored_table_node_int_float)),
        AliasNode::Make(ExpressionVector_(int_float2_a, int_float2_a), inner_aliases,
          ProjectionNode::Make(ExpressionVector_(int_float2_a, int_float2_a),
            stored_table_node_int_float2)))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasesUsedInView) {
  const auto [result_node, translation_info] =
      SqlToLqpHelper("CREATE VIEW alias_view AS SELECT a AS x, b as y FROM int_float WHERE a > 10");

  // clang-format off
  const auto aliases = std::vector<std::string>({"x", "y"});

  const auto view_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a, int_float_b), aliases,
    PredicateNode::Make(GreaterThan_(int_float_a, Value_(10)),
      stored_table_node_int_float));

  const auto view_columns = std::unordered_map<ColumnId, std::string>({
                                                                      {ColumnId{0}, "x"},
                                                                      {ColumnId{1}, "y"}
                                                                      });
  // clang-format on

  const auto view = std::make_shared<LqpWrapper>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::Make("alias_view", view, false);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListAliasesDifferentForSimilarColumnsUsedInView) {
  const auto [result_node, translation_info] =
      SqlToLqpHelper("CREATE VIEW alias_view AS SELECT a AS a1, a AS a2 FROM int_float WHERE a > 10");

  // clang-format off
  const auto aliases = std::vector<std::string>({"a1", "a2"});

  const auto view_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a, int_float_a), aliases,
    ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_a),
      PredicateNode::Make(GreaterThan_(int_float_a, Value_(10)),
        stored_table_node_int_float)));

  const auto view_columns = std::unordered_map<ColumnId, std::string>({
                                                                      {ColumnId{0}, "a1"},
                                                                      {ColumnId{1}, "a2"}
                                                                      });
  // clang-format on

  const auto view = std::make_shared<LqpWrapper>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::Make("alias_view", view, false);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SqlTranslatorTest, SelectListManyAliasesDifferentForSimilarColumnsUsedInView) {
  const auto [result_node, translation_info] =
      SqlToLqpHelper("CREATE VIEW alias_view (a3, a4) AS SELECT a AS a1, a AS a2 FROM int_float WHERE a > 10");

  // clang-format off
  const auto aliases = std::vector<std::string>({"a1", "a2"});

  const auto view_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a, int_float_a), aliases,
    ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_a),
      PredicateNode::Make(GreaterThan_(int_float_a, Value_(10)),
        stored_table_node_int_float)));

  const auto view_columns = std::unordered_map<ColumnId, std::string>({{ColumnId{0}, "a3"}, {ColumnId{1}, "a4"}});
  // clang-format on

  const auto view = std::make_shared<LqpWrapper>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::Make("alias_view", view, false);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SqlTranslatorTest, WhereSimple) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT a FROM int_float WHERE a < 200;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_a),
    PredicateNode::Make(LessThan_(int_float_a, 200), stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WhereWithArithmetics) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT a FROM int_float WHERE a * b >= b + a;");

  const auto a_times_b = Mul_(int_float_a, int_float_b);
  const auto b_plus_a = Add_(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_a),
    PredicateNode::Make(GreaterThanEquals_(a_times_b, b_plus_a),
      stored_table_node_int_float));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WhereWithLogical) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT a FROM int_float WHERE 5 >= b + a OR (a > 2 AND b > 2);");

  const auto b_plus_a = Add_(int_float_b, int_float_a);

  // clang-format off
  const auto predicate = Or_(GreaterThanEquals_(5, Add_(int_float_b, int_float_a)),
                             And_(GreaterThan_(int_float_a, 2), GreaterThan_(int_float_b, 2)));

  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_a),
    PredicateNode::Make(predicate,
      stored_table_node_int_float));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WhereWithBetween) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT a FROM int_float WHERE a BETWEEN b and 5;");

  const auto a_times_b = Mul_(int_float_a, int_float_b);
  const auto b_plus_a = Add_(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_a),
    PredicateNode::Make(BetweenInclusive_(int_float_a, int_float_b, 5),
      stored_table_node_int_float));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WhereIsNull) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT b FROM int_float WHERE a + b IS NULL;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_b),
    PredicateNode::Make(IsNull_(Add_(int_float_a, int_float_b)),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WhereIsNotNull) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT b FROM int_float WHERE a IS NOT NULL;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_b),
    PredicateNode::Make(IsNotNull_(int_float_a),
      stored_table_node_int_float));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WhereSimpleNotPredicate) {
  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper("SELECT * FROM int_float WHERE NOT (a = b);");
  const auto [actual_lqp_b, translation_info_b] = SqlToLqpHelper("SELECT * FROM int_float WHERE NOT (a != b);");
  const auto [actual_lqp_c, translation_info_c] = SqlToLqpHelper("SELECT * FROM int_float WHERE NOT (a > b);");
  const auto [actual_lqp_d, translation_info_d] = SqlToLqpHelper("SELECT * FROM int_float WHERE NOT (a < b);");
  const auto [actual_lqp_e, translation_info_e] = SqlToLqpHelper("SELECT * FROM int_float WHERE NOT (a >= b);");
  const auto [actual_lqp_f, translation_info_f] = SqlToLqpHelper("SELECT * FROM int_float WHERE NOT (a <= b);");
  const auto [actual_lqp_g, translation_info_g] = SqlToLqpHelper("SELECT * FROM int_float WHERE NOT (a IS NULL);");
  const auto [actual_lqp_h, translation_info_h] = SqlToLqpHelper("SELECT * FROM int_float WHERE NOT (a IS NOT NULL);");

  // clang-format off
  const auto expected_lqp_a = PredicateNode::Make(NotEquals_(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_b = PredicateNode::Make(Equals_(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_c = PredicateNode::Make(LessThanEquals_(int_float_a, int_float_b), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_d = PredicateNode::Make(GreaterThanEquals_(int_float_a, int_float_b), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_e = PredicateNode::Make(LessThan_(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_f = PredicateNode::Make(GreaterThan_(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_g = PredicateNode::Make(IsNotNull_(int_float_a), stored_table_node_int_float);
  const auto expected_lqp_h = PredicateNode::Make(IsNull_(int_float_a), stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp_d);
  EXPECT_LQP_EQ(actual_lqp_e, expected_lqp_e);
  EXPECT_LQP_EQ(actual_lqp_f, expected_lqp_f);
  EXPECT_LQP_EQ(actual_lqp_g, expected_lqp_g);
  EXPECT_LQP_EQ(actual_lqp_h, expected_lqp_h);
}

TEST_F(SqlTranslatorTest, AggregateWithGroupBy) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT SUM(a * 3) * b FROM int_float GROUP BY b");

  const auto a_times_3 = Mul_(int_float_a, 3);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Mul_(Sum_(a_times_3), int_float_b)),
    AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(Sum_(a_times_3)),
      ProjectionNode::Make(ExpressionVector_(a_times_3, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, AggregateWithGroupByAndHaving) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT b, SUM(a) AS s FROM int_float GROUP BY b HAVING s > 1000");

  const auto select_list_expressions = ExpressionVector_(int_float_b, Sum_(int_float_a));
  const auto aliases = std::vector<std::string>({"b", "s"});

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(select_list_expressions, aliases,
    PredicateNode::Make(GreaterThan_(Sum_(int_float_a), Value_(1000)),
      AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(Sum_(int_float_a)),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, AggregateWithGroupByAndUnrelatedHaving) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT b, COUNT(a) FROM int_float GROUP BY b HAVING SUM(a) > 1000");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_b, Count_(int_float_a)),
    PredicateNode::Make(GreaterThan_(Sum_(int_float_a), Value_(1000)),
      AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(Count_(int_float_a), Sum_(int_float_a)),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, Distinct) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT DISTINCT b FROM int_float");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, DistinctStar) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT DISTINCT * FROM int_float");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(int_float_a, int_float_b), ExpressionVector_(),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, DistinctAndGroupBy) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT DISTINCT b FROM int_float GROUP BY b");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(),
    AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, AggregateWithDistinctAndRelatedGroupBy) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT DISTINCT b, SUM(a * 3) * b FROM int_float GROUP BY b");

  const auto a_times_3 = Mul_(int_float_a, 3);

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(int_float_b, Mul_(Sum_(a_times_3), int_float_b)), ExpressionVector_(),
    AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(Sum_(a_times_3)),
      ProjectionNode::Make(ExpressionVector_(a_times_3, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, AggregateWithDistinctAndUnrelatedGroupBy) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT DISTINCT MIN(a) FROM int_float GROUP BY b");

  const auto a_times_3 = Mul_(int_float_a, 3);

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(Min_(int_float_a)), ExpressionVector_(),
    AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(Min_(int_float_a)),
    stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, AggregateCount) {
  const auto [actual_lqp_count_a, translation_info_1] = SqlToLqpHelper("SELECT b, COUNT(a) FROM int_float GROUP BY b");
  // clang-format off
  const auto expected_lqp_a =
  AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(Count_(int_float_a)),
    stored_table_node_int_float);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_a, expected_lqp_a);

  const auto [actual_lqp_count_star, translation_info_2] =
      SqlToLqpHelper("SELECT b, COUNT(*) FROM int_float GROUP BY b");
  // clang-format off
  const auto expected_lqp_star =
  AggregateNode::Make(ExpressionVector_(int_float_b), ExpressionVector_(CountStarLqp_(stored_table_node_int_float)),
    stored_table_node_int_float);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_star, expected_lqp_star);

  const auto [actual_lqp_count_distinct_a_plus_b, translation_info_3] =
      SqlToLqpHelper("SELECT a, b, COUNT(DISTINCT a + b) FROM int_float GROUP BY a, b");
  // clang-format off
  const auto expected_lqp_count_distinct_a_plus_b =
  AggregateNode::Make(ExpressionVector_(int_float_a, int_float_b), ExpressionVector_(CountDistinct_(Add_(int_float_a, int_float_b))),  // NOLINT
    ProjectionNode::Make(ExpressionVector_(Add_(int_float_a, int_float_b), int_float_a, int_float_b),
      stored_table_node_int_float));
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_distinct_a_plus_b, expected_lqp_count_distinct_a_plus_b);

  const auto [actual_lqp_count_1, translation_info_4] = SqlToLqpHelper("SELECT a, COUNT(1) FROM int_float GROUP BY a");
  // clang-format off
  const auto expected_lqp_count_1 =
  AggregateNode::Make(ExpressionVector_(int_float_a), ExpressionVector_(Count_(Value_(1))),
    ProjectionNode::Make(ExpressionVector_(Value_(1), int_float_a),
      stored_table_node_int_float));
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_1, expected_lqp_count_1);
}

TEST_F(SqlTranslatorTest, GroupByOnly) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT * FROM int_float GROUP BY b + 3, a / b, a, b");

  const auto b_plus_3 = Add_(int_float_b, 3);
  const auto a_divided_by_b = Div_(int_float_a, int_float_b);
  const auto group_by_expressions = ExpressionVector_(b_plus_3, a_divided_by_b, int_float_b);

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(b_plus_3, a_divided_by_b, int_float_a, int_float_b), ExpressionVector_(),
    ProjectionNode::Make(ExpressionVector_(b_plus_3, a_divided_by_b, int_float_a, int_float_b),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, AggregateAndGroupByWildcard) {
  // - y is an alias assigned in the SELECT list and can be used in the GROUP BY list
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT int_float.*, b+3 AS y, SUM(a+b) FROM int_float GROUP BY a, y, b");

  const auto sum_a_plus_b = Sum_(Add_(int_float_a, int_float_b));
  const auto b_plus_3 = Add_(int_float_b, 3);

  const auto aliases = std::vector<std::string>({"a", "b", "y", "SUM(a + b)"});
  const auto select_list_expressions =
      ExpressionVector_(int_float_a, int_float_b, b_plus_3, Sum_(Add_(int_float_a, int_float_b)));

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(select_list_expressions, aliases,
    ProjectionNode::Make(select_list_expressions,
      AggregateNode::Make(ExpressionVector_(int_float_a, b_plus_3, int_float_b), ExpressionVector_(sum_a_plus_b),
        ProjectionNode::Make(ExpressionVector_(Add_(int_float_a, int_float_b), int_float_a, b_plus_3, int_float_b),
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, AggregateAndGroupByWildcardTwoTables) {
  // - y is an alias assigned in the SELECT list and can be used in the GROUP BY list
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT t1.*, t2.a, SUM(t2.b) FROM int_float t1, int_float t2 GROUP BY t1.a, t1.b, t2.a");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(int_float_a, int_float_b, int_float_a), ExpressionVector_(Sum_(int_float_b)),
    JoinNode::Make(JoinMode::kCross, stored_table_node_int_float, stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, AggregateForwarding) {
  // Test that a referenced Aggregate does not result in redundant (and illegal!) AggregateNodes

  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT x + 3 FROM (SELECT MIN(a) as x FROM int_float) AS t;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Min_(int_float_a), 3)),
    AliasNode::Make(ExpressionVector_(Min_(int_float_a)), std::vector<std::string>({"x"}),
      AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Min_(int_float_a)),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, ProjectedAggregateForwarding) {
  // Test that a referenced Aggregate does not result in redundant (and illegal!) AggregateNodes

  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT x + 3 FROM (SELECT MIN(a) - 1 as x FROM int_float) AS t;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Sub_(Min_(int_float_a), 1), 3)),
    AliasNode::Make(ExpressionVector_(Sub_(Min_(int_float_a), 1)), std::vector<std::string>({"x"}),
      ProjectionNode::Make(ExpressionVector_(Sub_(Min_(int_float_a), 1)),
        AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Min_(int_float_a)),
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, SubqueryFromSimple) {
  const auto [actual_lqp_a, translation_info_a] =
      SqlToLqpHelper("SELECT z.x, z.a, z.b FROM (SELECT a + b AS x, * FROM int_float) AS z");
  const auto [actual_lqp_b, translation_info_b] =
      SqlToLqpHelper("SELECT * FROM (SELECT a + b AS x, * FROM int_float) AS z");
  const auto [actual_lqp_c, translation_info_c] =
      SqlToLqpHelper("SELECT z.* FROM (SELECT a + b AS x, * FROM int_float) AS z");

  const auto expressions = ExpressionVector_(Add_(int_float_a, int_float_b), int_float_a, int_float_b);
  const auto aliases = std::vector<std::string>({"x", "a", "b"});

  // Hyrise #1186: Redundant AliasNode due to the SqlTranslator architecture.
  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(expressions, aliases,
    AliasNode::Make(expressions, aliases,
      ProjectionNode::Make(expressions, stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp);
}

TEST_F(SqlTranslatorTest, OrderByTest) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT * FROM int_float ORDER BY a, a+b DESC, b ASC");

  const auto sort_modes = std::vector<SortMode>({SortMode::kAscending, SortMode::kDescending, SortMode::kAscending});

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_b),
    SortNode::Make(ExpressionVector_(int_float_a, Add_(int_float_a, int_float_b), int_float_b), sort_modes,
      ProjectionNode::Make(ExpressionVector_(Add_(int_float_a, int_float_b), int_float_a, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, JoinSimple) {
  const auto [actual_lqp_a, translation_info_a] =
      SqlToLqpHelper("SELECT * FROM int_float JOIN int_float2 ON int_float2.a > int_float.a");
  const auto [actual_lqp_b, translation_info_b] =
      SqlToLqpHelper("SELECT * FROM int_float LEFT JOIN int_float2 ON int_float2.a > int_float.a");
  const auto [actual_lqp_c, translation_info_c] =
      SqlToLqpHelper("SELECT * FROM int_float RIGHT JOIN int_float2 ON int_float2.a > int_float.a");
  const auto [actual_lqp_d, translation_info_d] =
      SqlToLqpHelper("SELECT * FROM int_float FULL OUTER JOIN int_float2 ON int_float2.a > int_float.a");

  const auto a_gt_a = GreaterThan_(int_float2_a, int_float_a);
  const auto node_a = stored_table_node_int_float;
  const auto node_b = stored_table_node_int_float2;

  const auto expected_lqp_a = JoinNode::Make(JoinMode::kInner, a_gt_a, node_a, node_b);
  const auto expected_lqp_b = JoinNode::Make(JoinMode::kLeftOuter, a_gt_a, node_a, node_b);
  const auto expected_lqp_c = JoinNode::Make(JoinMode::kRightOuter, a_gt_a, node_a, node_b);
  const auto expected_lqp_d = JoinNode::Make(JoinMode::kFullOuter, a_gt_a, node_a, node_b);

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp_d);
}

TEST_F(SqlTranslatorTest, JoinCrossSelectStar) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT * FROM int_float, int_float2 AS t, int_float5 WHERE t.a < 2");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::Make(LessThan_(int_float2_a, 2),
    JoinNode::Make(JoinMode::kCross,
      JoinNode::Make(JoinMode::kCross,
        stored_table_node_int_float,
        stored_table_node_int_float2),
    stored_table_node_int_float5));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, JoinCrossSelectElements) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT int_float5.d, t.* FROM int_float, int_float2 AS t, int_float5 WHERE t.a < 2");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float5_d, int_float2_a, int_float2_b),
    PredicateNode::Make(LessThan_(int_float2_a, 2),
      JoinNode::Make(JoinMode::kCross,
        JoinNode::Make(JoinMode::kCross,
          stored_table_node_int_float,
          stored_table_node_int_float2),
      stored_table_node_int_float5)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, JoinLeftRightFullOuter) {
  const auto [actual_lqp_left, translation_info_1] =
      SqlToLqpHelper("SELECT * FROM int_float AS a LEFT JOIN int_float2 AS b ON a.a = b.a;");

  // clang-format off
  const auto expected_lqp_left =
  JoinNode::Make(JoinMode::kLeftOuter, Equals_(int_float_a, int_float2_a),
    stored_table_node_int_float,
    stored_table_node_int_float2);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_left, expected_lqp_left);

  const auto [actual_lqp_right, translation_info_2] =
      SqlToLqpHelper("SELECT * FROM int_float AS a RIGHT JOIN int_float2 AS b ON a.a = b.a;");

  // clang-format off
  const auto expected_lqp_right =
  JoinNode::Make(JoinMode::kRightOuter, Equals_(int_float_a, int_float2_a),
    stored_table_node_int_float,
    stored_table_node_int_float2);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_right, expected_lqp_right);

  const auto [actual_lqp_full, translation_info_3] =
      SqlToLqpHelper("SELECT * FROM int_float AS a FULL JOIN int_float2 AS b ON a.a = b.a;");

  // clang-format off
  const auto expected_lqp_full_outer =
  JoinNode::Make(JoinMode::kFullOuter, Equals_(int_float_a, int_float2_a),
    stored_table_node_int_float,
    stored_table_node_int_float2);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_full, expected_lqp_full_outer);
}

TEST_F(SqlTranslatorTest, JoinSemiOuterPredicatesForNullSupplyingSide) {
  // Test that predicates in the JOIN condition that reference only the null-supplying side are pushed down

  const auto [actual_lqp_left, translation_info_1] = SqlToLqpHelper(
      "SELECT"
      "  * "
      "FROM "
      "  int_float AS a LEFT JOIN int_float2 AS b "
      "    ON b.a > 5 AND a.a = b.a "
      "WHERE b.b < 2;");

  // clang-format off

  const auto expected_lqp_left =
  PredicateNode::Make(LessThan_(int_float2_b, 2),
      JoinNode::Make(JoinMode::kLeftOuter, Equals_(int_float_a, int_float2_a),
        stored_table_node_int_float,
        PredicateNode::Make(GreaterThan_(int_float2_a, 5),
          stored_table_node_int_float2)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_left, expected_lqp_left);

  const auto [actual_lqp_right, translation_info_2] = SqlToLqpHelper(
      "SELECT"
      "  * "
      "FROM "
      "  int_float AS a RIGHT JOIN int_float2 AS b "
      "    ON a.a > 5 AND a.a = b.a "
      "WHERE b.b < 2;");

  // clang-format off

  const auto expected_lqp_right =
  PredicateNode::Make(LessThan_(int_float2_b, 2),
      JoinNode::Make(JoinMode::kRightOuter, Equals_(int_float_a, int_float2_a),
        PredicateNode::Make(GreaterThan_(int_float_a, 5),
          stored_table_node_int_float),
        stored_table_node_int_float2));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_right, expected_lqp_right);
}

TEST_F(SqlTranslatorTest, JoinOuterPredicatesForNullPreservingSide) {
  // See Hyrise #1436

  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM int_float AS a LEFT JOIN int_float2 AS b ON a.a > 5 AND a.a = b.a"),
               InvalidInputException);

  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM int_float AS a RIGHT JOIN int_float2 AS b ON b.a > 5 AND a.a = b.a"),
               InvalidInputException);

  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM int_float AS a FULL JOIN int_float2 AS b ON a.a > 5 AND a.a = b.a"),
               InvalidInputException);

  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM int_float AS a FULL JOIN int_float2 AS b ON b.a > 5 AND a.a = b.a"),
               InvalidInputException);
}

TEST_F(SqlTranslatorTest, JoinNaturalSimple) {
  // Also test that columns can be referenced after a natural join

  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "SELECT "
      "  * "
      "FROM "
      "  int_float AS a NATURAL JOIN int_float2 AS b "
      "WHERE "
      "  a.b > 10 AND a.a > 5");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::Make(GreaterThan_(int_float_b, 10),
    PredicateNode::Make(GreaterThan_(int_float_a, 5),
      ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_b),
        PredicateNode::Make(Equals_(int_float_b, int_float2_b),
          JoinNode::Make(JoinMode::kInner, Equals_(int_float_a, int_float2_a),
            stored_table_node_int_float,
            stored_table_node_int_float2)))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, JoinNaturalColumnAlias) {
  // Test that the Natural join can work with column aliases and that the output columns have the correct name

  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "SELECT "
      "  * "
      "FROM "
      "  int_float AS a NATURAL JOIN (SELECT a AS d, b AS a, c FROM int_int_int) AS b");

  const auto aliases = std::vector<std::string>{{"a", "b", "d", "c"}};
  const auto subquery_aliases = std::vector<std::string>{{"d", "a", "c"}};

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a, int_float_b, int_int_int_a, int_int_int_c), aliases,
    ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_b, int_int_int_a, int_int_int_c),
      JoinNode::Make(JoinMode::kInner, Equals_(int_float_a, int_int_int_b),
        stored_table_node_int_float,
        AliasNode::Make(ExpressionVector_(int_int_int_a, int_int_int_b, int_int_int_c), subquery_aliases,
          stored_table_node_int_int_int))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, JoinInnerComplexPredicateA) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "SELECT * FROM int_float JOIN int_float2 ON int_float.a + int_float2.a = int_float2.b * int_float.a;");

  // clang-format off
  const auto a_plus_a = Add_(int_float_a, int_float2_a);
  const auto b_times_a = Mul_(int_float2_b, int_float_a);
  const auto expected_lqp =
  PredicateNode::Make(Equals_(a_plus_a, b_times_a),
    JoinNode::Make(JoinMode::kCross,
      stored_table_node_int_float,
      stored_table_node_int_float2));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, JoinInnerComplexPredicateB) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT * FROM int_float AS m1 JOIN int_float AS m2 ON m1.a * 3 = m2.a - 5 OR m1.a > 20;");

  // clang-format off
  const auto a_times_3 = Mul_(int_float_a, 3);
  const auto a_minus_5 = Sub_(int_float_a, 5);

  const auto join_cross = JoinNode::Make(JoinMode::kCross, stored_table_node_int_float, stored_table_node_int_float);
  const auto join_predicate = Or_(Equals_(Mul_(int_float_a, 3), Sub_(int_float_a, 5)), GreaterThan_(int_float_a, 20));

  const auto expected_lqp =
  PredicateNode::Make(join_predicate,
    JoinNode::Make(JoinMode::kCross,
      stored_table_node_int_float,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, FromColumnAliasingSimple) {
  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper("SELECT t.x FROM int_float AS t (x, y) WHERE x = t.y");
  const auto [actual_lqp_b, translation_info_b] =
      SqlToLqpHelper("SELECT t.x FROM (SELECT * FROM int_float) AS t (x, y) WHERE x = t.y");

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a), std::vector<std::string>({"x", }),
    ProjectionNode::Make(ExpressionVector_(int_float_a),
      PredicateNode::Make(Equals_(int_float_a, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SqlTranslatorTest, FromColumnAliasingAggregation) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("SELECT foo + 1 FROM (SELECT a, MIN(b) FROM int_float WHERE a > 10 GROUP BY a) AS t (bar, foo)");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(Min_(int_float_b), 1)),
    AggregateNode::Make(ExpressionVector_(int_float_a), ExpressionVector_(Min_(int_float_b)),
      PredicateNode::Make(GreaterThan_(int_float_a, 10),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, FromColumnAliasingColumnsSwitchNames) {
  // Tricky: Columns "switch names". a becomes b and b becomes a

  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper("SELECT * FROM int_float AS t (b, a) WHERE b = t.a");
  const auto [actual_lqp_b, translation_info_b] =
      SqlToLqpHelper("SELECT * FROM (SELECT * FROM int_float) AS t (b, a) WHERE b = t.a");

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(int_float_a, int_float_b), std::vector<std::string>({"b", "a"}),
    PredicateNode::Make(Equals_(int_float_a, int_float_b),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SqlTranslatorTest, FromColumnAliasingTablesSwitchNames) {
  // Tricky: Tables "switch names". int_float becomes int_float2 and int_float2 becomes int_float

  const auto [actual_lqp_a, translation_info_a] = SqlToLqpHelper(
      "SELECT int_float.y, int_float2.* "
      "FROM int_float AS int_float2 (a, b), int_float2 AS int_float(x,y) "
      "WHERE int_float.x = int_float2.b");
  const auto [actual_lqp_b, translation_info_b] = SqlToLqpHelper(
      "SELECT int_float.y, int_float2.* "
      "FROM (SELECT * FROM int_float) AS int_float2 (a, b), (SELECT * FROM int_float2) AS int_float(x,y) "
      "WHERE int_float.x = int_float2.b");

  // clang-format off
  const auto expected_lqp =
  AliasNode::Make(ExpressionVector_(int_float2_b, int_float_a, int_float_b), std::vector<std::string>({"y", "a", "b"}),
    ProjectionNode::Make(ExpressionVector_(int_float2_b, int_float_a, int_float_b),
      PredicateNode::Make(Equals_(int_float2_a, int_float_b),
        JoinNode::Make(JoinMode::kCross,
          stored_table_node_int_float,
          stored_table_node_int_float2))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SqlTranslatorTest, SameColumnForDifferentTableNames) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT R.a, S.a FROM int_float AS R, int_float AS S");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_a),
    JoinNode::Make(JoinMode::kCross,
      stored_table_node_int_float,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, LimitLiteral) {
  // Most common case: LIMIT to a fixed number
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT * FROM int_float LIMIT 1;");
  const auto expected_lqp = LimitNode::Make(Value_(1), stored_table_node_int_float);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, Extract) {
  std::vector<DatetimeComponent> components{DatetimeComponent::kYear,   DatetimeComponent::kMonth,
                                            DatetimeComponent::kDay,    DatetimeComponent::kHour,
                                            DatetimeComponent::kMinute, DatetimeComponent::kSecond};

  std::shared_ptr<skyrise::AbstractLqpNode> actual_lqp;
  std::shared_ptr<skyrise::AbstractLqpNode> expected_lqp;
  ProjectionNode::Make(ExpressionVector_(Extract_(DatetimeComponent::kYear, "1993-08-01")), DummyTableNode::Make());

  for (const auto& component : components) {
    std::stringstream query_str;
    query_str << "SELECT EXTRACT(" << component << " FROM '1993-08-01');";

    const auto [actual_lqp, translation_info] = SqlToLqpHelper(query_str.str());
    // clang-format off
    expected_lqp =
    ProjectionNode::Make(ExpressionVector_(Extract_(component, Value_("1993-08-01"))),
      DummyTableNode::Make());
    // clang-format on

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SqlTranslatorTest, UnaryMinus) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper("SELECT -a FROM int_float");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::Make(ExpressionVector_(UnaryMinus_(int_float_a)),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, CreateView) {
  const auto query = "CREATE VIEW my_first_view AS SELECT a, b, a + b, a*b AS t FROM int_float WHERE a = 'b';"s;
  const auto [result_node, translation_info] = SqlToLqpHelper(query);

  // clang-format off
  const auto select_list_expressions = ExpressionVector_(int_float_a, int_float_b, Add_(int_float_a, int_float_b), Mul_(int_float_a, int_float_b));  // NOLINT

  const auto view_lqp =
  AliasNode::Make(select_list_expressions, std::vector<std::string>({"a", "b", "a + b", "t"}),
    ProjectionNode::Make(select_list_expressions,
      PredicateNode::Make(Equals_(int_float_a, Value_("b")),
         stored_table_node_int_float)));

  const auto view_columns = std::unordered_map<ColumnId, std::string>({
                                                                      {ColumnId{0}, "a"},
                                                                      {ColumnId{1}, "b"},
                                                                      {ColumnId{3}, "t"},
                                                                      });
  // clang-format on

  const auto view = std::make_shared<LqpWrapper>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::Make("my_first_view", view, false);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SqlTranslatorTest, CreateAliasView) {
  const auto [actual_lqp, translation_info] =
      SqlToLqpHelper("CREATE VIEW my_second_view (c, d) AS SELECT * FROM int_float WHERE a = 'b';");

  // clang-format off
  const auto view_columns = std::unordered_map<ColumnId, std::string>({
                                                                      {ColumnId{0}, "c"},
                                                                      {ColumnId{1}, "d"}
                                                                      });

  const auto view_lqp = PredicateNode::Make(Equals_(int_float_a, Value_("b")), stored_table_node_int_float);
  // clang-format on

  const auto view = std::make_shared<LqpWrapper>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::Make("my_second_view", view, false);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, CreateViewIfNotExists) {
  const auto query = "CREATE VIEW IF NOT EXISTS my_first_view AS SELECT b, a FROM int_float WHERE a = 'b';"s;
  const auto [result_node, translation_info] = SqlToLqpHelper(query);

  const auto select_list_expressions = ExpressionVector_(int_float_b, int_float_a);

  // clang-format off
  const auto view_lqp =
  ProjectionNode::Make(select_list_expressions,
    PredicateNode::Make(Equals_(int_float_a, Value_("b")),
      stored_table_node_int_float));
  // clang-format on

  const auto view_columns = std::unordered_map<ColumnId, std::string>({{ColumnId{0}, "b"}, {ColumnId{1}, "a"}});

  const auto view = std::make_shared<LqpWrapper>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::Make("my_first_view", view, true);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SqlTranslatorTest, DropView) {
  const auto query = "DROP VIEW my_third_view"s;
  auto [result_node, translation_info] = SqlToLqpHelper(query);

  const auto lqp = DropViewNode::Make("my_third_view", false);

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SqlTranslatorTest, DropViewIfExists) {
  const auto query = "DROP VIEW IF EXISTS my_third_view"s;
  auto [result_node, translation_info] = SqlToLqpHelper(query);

  const auto lqp = DropViewNode::Make("my_third_view", true);

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SqlTranslatorTest, IntLimitsAndUnaryMinus) {
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1 + 2").first,
                ProjectionNode::Make(ExpressionVector_(Add_(1, 2)), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1 + -2").first,
                ProjectionNode::Make(ExpressionVector_(Add_(1, UnaryMinus_(2))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1 + - 2").first,
                ProjectionNode::Make(ExpressionVector_(Add_(1, UnaryMinus_(2))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1 +-2").first,
                ProjectionNode::Make(ExpressionVector_(Add_(1, UnaryMinus_(2))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1+-2").first,
                ProjectionNode::Make(ExpressionVector_(Add_(1, UnaryMinus_(2))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1+9223372036854775807").first,
                ProjectionNode::Make(ExpressionVector_(Add_(1, static_cast<int64_t>(9223372036854775807ll))),
                                     DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(
      SqlToLqpHelper("SELECT 1+-9223372036854775807").first,
      ProjectionNode::Make(ExpressionVector_(Add_(1, UnaryMinus_(static_cast<int64_t>(9223372036854775807ll)))),
                           DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1+-9223372036854775808").first,
                ProjectionNode::Make(ExpressionVector_(Add_(1, std::numeric_limits<int64_t>::min())),
                                     DummyTableNode::Make()));  // NOLINT
  EXPECT_ANY_THROW(SqlToLqpHelper("SELECT 9223372036854775808"));
  EXPECT_ANY_THROW(SqlToLqpHelper("SELECT 1-9223372036854775808"));
}

TEST_F(SqlTranslatorTest, OperatorPrecedence) {
  /**
   * Though the operator precedence is handled by the sql-parser, do some checks here as well that it works as expected.
   * SQLite is our reference: https://www.sqlite.org/lang_expr.html
   * For operators with the same precedence, we evaluate left-to-right
   */

  // clang-format off
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1 + 2 * 3 / -4").first, ProjectionNode::Make(ExpressionVector_(Add_(1, Div_(Mul_(2, 3), UnaryMinus_(4)))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1 + 2 * 3 / 4").first, ProjectionNode::Make(ExpressionVector_(Add_(1, Div_(Mul_(2, 3), 4))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 3 + 5 % 3").first, ProjectionNode::Make(ExpressionVector_(Add_(3, Mod_(5, 3))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 3 + 5 > 4 / 2").first, ProjectionNode::Make(ExpressionVector_(GreaterThan_(Add_(3, 5), Div_(4, 2))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 5 < 3 == 2 < 1").first, ProjectionNode::Make(ExpressionVector_(Equals_(LessThan_(5, 3), LessThan_(2, 1))), DummyTableNode::Make()));  // NOLINT
  EXPECT_LQP_EQ(SqlToLqpHelper("SELECT 1 OR 2 AND 3 OR 4").first, ProjectionNode::Make(ExpressionVector_(Or_(Or_(1, And_(2, 3)), 4)), DummyTableNode::Make()));  // NOLINT
  // clang-format on
}

TEST_F(SqlTranslatorTest, CatchInputErrors) {
  EXPECT_THROW(SqlToLqpHelper("SELECT no_such_table.* FROM int_float;"), InvalidInputException);
  // TODO(anyone): Enable, when FunctionExpression is supported.
  // EXPECT_THROW(SqlToLqpHelper("SELECT no_such_function(5+3);"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT no_such_column FROM int_float;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM no_such_table;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT SUM(b) FROM int_string;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT a FROM int_string GROUP BY a HAVING SUM(b) > 2;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT b, SUM(b) AS s FROM table_a GROUP BY a;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM int_float GROUP BY a;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT t1.*, t2.*, SUM(t2.b) FROM int_float t1, int_float t2 GROUP BY t1.a, t1.b, t2.a"),
               InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM table_a JOIN table_b ON a = b;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM table_a JOIN table_b ON table_a.a = table_b.a AND a = 3;"),
               InvalidInputException);  // NOLINT
  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM int_float WHERE 3 + 4;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT a AS b FROM int_float WHERE b > 5"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT a AS b FROM int_float GROUP BY int_float.b"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT a AS b, b AS a FROM int_float WHERE a > 5"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT a, SUM(b) FROM int_float GROUP BY a HAVING b > 10;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM int_float LIMIT 1 OFFSET 1;"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("WITH q AS (SELECT * FROM int_float), q AS (SELECT b FROM q) SELECT * FROM q;"),
               InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("COPY no_such_table TO 'a_file.tbl';"), InvalidInputException);
  EXPECT_THROW(SqlToLqpHelper("SELECT * FROM meta_unknown;"), InvalidInputException);
}

TEST_F(SqlTranslatorTest, WithClauseSingleQuerySimple) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "WITH "
      "wq AS (SELECT a, b FROM int_int_int) "
      "SELECT * FROM wq WHERE a > 123;");

  // clang-format off
  const auto wq_lqp =
    ProjectionNode::Make(ExpressionVector_(int_int_int_a, int_int_int_b),
      stored_table_node_int_int_int);

  const auto expected_lqp =
    PredicateNode::Make(GreaterThan_(int_int_int_a, Value_(123)),
      wq_lqp);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WithClauseSingleQueryAlias) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "WITH "
      "wq AS (SELECT a AS x FROM int_int_int) "
      "SELECT * FROM wq WHERE x > 123;");

  // clang-format off
  const auto aliases = std::vector<std::string>{"x"};
  const auto expressions = ExpressionVector_(int_int_int_a);
  const auto wq_lqp =
    AliasNode::Make(expressions, aliases,
      ProjectionNode::Make(ExpressionVector_(int_int_int_a),
        stored_table_node_int_int_int));

  const auto expected_lqp =
    AliasNode::Make(expressions, aliases,
      PredicateNode::Make(GreaterThan_(int_int_int_a, Value_(123)),
        wq_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WithClauseSingleQueryAliasWhere) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "WITH "
      "wq AS (SELECT a AS x FROM int_int_int WHERE a > 123) "
      "SELECT x AS z FROM wq;");

  // clang-format off
  const auto alias_x = std::vector<std::string>{"x"};
  const auto wq_lqp =
    AliasNode::Make(ExpressionVector_(int_int_int_a), alias_x,
      ProjectionNode::Make(ExpressionVector_(int_int_int_a),
        PredicateNode::Make(GreaterThan_(int_int_int_a, Value_(123)),
          stored_table_node_int_int_int)));

  const auto alias_z = std::vector<std::string>{"z"};
  const auto expected_lqp =
    AliasNode::Make(ExpressionVector_(int_int_int_a), alias_z,
      wq_lqp);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WithClauseSingleQueryAggregateGroupBy) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "WITH "
      "wq AS (SELECT a, SUM(b) FROM int_int_int GROUP BY a) "
      "SELECT * FROM wq;");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::Make(ExpressionVector_(int_int_int_a), ExpressionVector_(Sum_(int_int_int_b)),
    stored_table_node_int_int_int);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WithClauseSingleQueryAggregateGroupByAlias) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "WITH "
      "wq AS (SELECT a, SUM(b) AS sum FROM int_int_int GROUP BY a) "
      "SELECT * FROM wq WHERE sum > 10;");

  // clang-format off
  const auto sum_b = Sum_(int_int_int_b);
  const auto select_list_expressions = ExpressionVector_(int_int_int_a, sum_b);
  const auto aliases = std::vector<std::string>{"a", "sum"};
  const auto wq_lqp =
    AliasNode::Make(select_list_expressions, aliases,
      AggregateNode::Make(ExpressionVector_(int_int_int_a), ExpressionVector_(sum_b),
        stored_table_node_int_int_int));

  // Hyrise #1186: Redundant AliasNode due to the SqlTranslator architecture.
  const auto expected_lqp =
    AliasNode::Make(select_list_expressions, aliases,
      PredicateNode::Make(GreaterThan_(sum_b, Value_(10)),
        wq_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WithClauseDoubleQuery) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "WITH "
      "wq1 AS (SELECT a AS a1, b AS b1 FROM int_float), "
      "wq2 AS (SELECT a AS a2, b AS b2 FROM int_float2) "
      "SELECT * FROM wq1 JOIN wq2 ON a1 = a2;");
  // clang-format off
  const auto expressions_wq1 = ExpressionVector_(int_float_a, int_float_b);
  const auto aliases_wq1 = std::vector<std::string>{"a1", "b1"};
  const auto wq1_lqp =
    AliasNode::Make(expressions_wq1, aliases_wq1,
      stored_table_node_int_float);

  const auto expressions_wq2 = ExpressionVector_(int_float2_a, int_float2_b);
  const auto aliases_wq2 = std::vector<std::string>{"a2", "b2"};
  const auto wq2_lqp =
    AliasNode::Make(expressions_wq2, aliases_wq2,
      stored_table_node_int_float2);

  const auto expressions_join = ExpressionVector_(int_float_a, int_float_b, int_float2_a, int_float2_b);
  const auto aliases_join = std::vector<std::string>({"a1", "b1", "a2", "b2"});
  const auto a1_equals_a2 = Equals_(int_float_a, int_float2_a);
  const auto expected_lqp =
    AliasNode::Make(expressions_join, aliases_join,
      JoinNode::Make(JoinMode::kInner, a1_equals_a2, wq1_lqp, wq2_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WithClauseConsecutiveQueriesSimple) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "WITH "
      "wq1 AS (SELECT a, b FROM int_int_int), "
      "wq2 AS (SELECT b FROM wq1) "
      "SELECT * FROM wq2;");

  // clang-format off
  const auto wq1_lqp =
    ProjectionNode::Make(ExpressionVector_(int_int_int_a, int_int_int_b),
      stored_table_node_int_int_int);
  const auto wq2_lqp =
    ProjectionNode::Make(ExpressionVector_(int_int_int_b),
      wq1_lqp);

  const auto& expected_lqp = wq2_lqp;
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WithClauseConsecutiveQueriesWhereAlias) {
  const auto [actual_lqp, translation_info] = SqlToLqpHelper(
      "WITH "
      "wq1 AS (SELECT a, b FROM int_int_int WHERE a > 9), "
      "wq2 AS (SELECT b AS z FROM wq1 WHERE b >= 10) "
      "SELECT * FROM wq2;");

  // clang-format off
  const auto wq1_lqp =
    ProjectionNode::Make(ExpressionVector_(int_int_int_a, int_int_int_b),
      PredicateNode::Make(GreaterThan_(int_int_int_a, Value_(9)),
        stored_table_node_int_int_int));

  const auto alias_z = std::vector<std::string>{"z"};
  const auto wq2_lqp =
    AliasNode::Make(ExpressionVector_(int_int_int_b), alias_z,
      ProjectionNode::Make(ExpressionVector_(int_int_int_b),
        PredicateNode::Make(GreaterThanEquals_(int_int_int_b, Value_(10)),
          wq1_lqp)));


  // Hyrise #1186: Redundant AliasNode due to the SqlTranslator architecture.
  const auto expected_lqp =
    AliasNode::Make(ExpressionVector_(int_int_int_b), alias_z,
      wq2_lqp);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, WithClauseTableMasking) {
  // Check StorageManager for existance of table int_float
  const auto [pre_condition_lqp_actual, translation_info_1] = SqlToLqpHelper("SELECT * FROM int_float;");
  const auto pre_condition_lqp_expected = stored_table_node_int_float;
  EXPECT_LQP_EQ(pre_condition_lqp_actual, pre_condition_lqp_expected);

  // Mask StorageManager's int_float table via WITH clause
  const auto [actual_lqp, translation_info_2] = SqlToLqpHelper(
      "WITH "
      "int_float AS (SELECT a, b FROM int_int_int) "
      "SELECT * FROM int_float;");

  // clang-format off
  const auto expected_lqp =
    ProjectionNode::Make(ExpressionVector_(int_int_int_a, int_int_int_b),
      stored_table_node_int_int_int);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SqlTranslatorTest, CopyStatementImport) {
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY a_table FROM 'a_file.tbl';");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Auto);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY a_table FROM 'a_file.tbl' WITH FORMAT TBL;");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Tbl);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY a_table FROM 'a_file.tbl' WITH FORMAT CSV;");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Csv);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY a_table FROM 'a_file.tbl' WITH FORMAT BINARY;");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Binary);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY a_table FROM 'a_file.tbl' WITH FORMAT BIN;");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Binary);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SqlTranslatorTest, CopyStatementExport) {
  // clang-format off
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY int_float TO 'a_file.tbl';");
    const auto expected_lqp = ExportNode::Make("int_float", "a_file.tbl", FileType::Auto, stored_table_node_int_float); //NOLINT
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY int_float TO 'a_file.tbl';");
    const auto expected_lqp =
      ExportNode::Make("int_float", "a_file.tbl", FileType::Auto,
        stored_table_node_int_float);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY int_float TO 'a_file.tbl' WITH FORMAT TBL;");
    const auto expected_lqp = ExportNode::Make("int_float", "a_file.tbl", FileType::Tbl, stored_table_node_int_float);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY int_float TO 'a_file.tbl' WITH FORMAT CSV;");
    const auto expected_lqp = ExportNode::Make("int_float", "a_file.tbl", FileType::Csv, stored_table_node_int_float);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY int_float TO 'a_file.tbl' WITH FORMAT BINARY;");
    const auto expected_lqp = ExportNode::Make("int_float", "a_file.tbl", FileType::Binary, stored_table_node_int_float);  // NOLINT
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("COPY int_float TO 'a_file.tbl' WITH FORMAT BIN;");
    const auto expected_lqp = ExportNode::Make("int_float", "a_file.tbl", FileType::Binary, stored_table_node_int_float);  // NOLINT
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  // clang-format on
}

TEST_F(SqlTranslatorTest, ImportStatement) {
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("IMPORT FROM TBL FILE 'a_file.tbl' INTO a_table;");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Tbl);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("IMPORT FROM CSV FILE 'a_file.tbl' INTO a_table;");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Csv);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("IMPORT FROM BINARY FILE 'a_file.tbl' INTO a_table;");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Binary);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    const auto [actual_lqp, translation_info] = SqlToLqpHelper("IMPORT FROM BIN FILE 'a_file.tbl' INTO a_table;");
    const auto expected_lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Binary);
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

}  // namespace skyrise
