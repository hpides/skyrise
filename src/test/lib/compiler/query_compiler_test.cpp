/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/query_compiler.hpp"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "client/client.hpp"
#include "compiler/logical_query_plan/alias_node.hpp"
#include "compiler/logical_query_plan/join_node.hpp"
#include "compiler/logical_query_plan/predicate_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "expression/expression_functional.hpp"
#include "metadata/remote_catalog.hpp" // TODO Adopt glue_catalog.hpp
#include "metadata/table_schema.hpp"
#include "metadata/tpch_mock_catalog.hpp"
#include "testing/testing_assert.hpp"
#include "tpch/tpch_query_generator.hpp"
//#include "visualization/lqp_visualizer.hpp"
//#include "visualization/pqp_visualizer.hpp"

namespace {
using namespace skyrise;  // NOLINT(google-build-using-namespace)

const std::string kTableNameLineitem = "lineitem";
const std::string kTpchDatabaseNameSF1000 = "CI_TPCH_SF1000_Database";

// Consider removing visualization stuff – relict from master's thesis.
void VisualizePlans(QueryCompiler& query_compiler, const std::string& query_name) {
  std::cout << *query_compiler.GetOptimizedLqps().front() << std::endl;
  std::cout << *query_compiler.GetOptimizedPqps().front() << std::endl;

  // Create SVG plan visualization

//  const std::string prefix = "Plan_" + query_name;
//  GraphvizConfig graphviz_config;
//  graphviz_config.format = "svg";
//  LqpVisualizer{graphviz_config}.Visualize(query_compiler.GetLqps(), prefix + "_LQP." + format);
//  LqpVisualizer{graphviz_config}.Visualize(query_compiler.GetOptimizedLqps(), prefix + "_LQP_optimized." + format);
//  PqpVisualizer{graphviz_config}.Visualize(query_compiler.GetPqps(), prefix + "_PQP." + format);
//  PqpVisualizer{graphviz_config}.Visualize(query_compiler.GetOptimizedPqps(), prefix + "_PQP_optimized." + format);
//  for (const auto& pipeline : query_compiler.GetPqpPipelines()) {
//    std::string file_name = prefix;
//    file_name.append("_").append(pipeline->Identity()).append(".").append(format);
//    PqpVisualizer{graphviz_config}.Visualize(pipeline->GetFragments(), file_name);
//  }
}

}  // namespace

namespace skyrise {
using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class QueryCompilerTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    catalog_ = std::make_shared<TpchMockCatalog>();
    catalog_->AddTableSchemaFromFileHeader("table_a", "resources/test_data/tbl/int_float.tbl");
    catalog_->AddTableSchemaFromFileHeader("table_b", "resources/test_data/tbl/int_float2.tbl");
  }

 protected:
  static inline std::shared_ptr<MockCatalog> catalog_;
  // clang-format off
  const std::string join_query_ = R"(SELECT table_a.a, table_a.b, table_b.b AS bb
                                     FROM table_a, table_b
                                     WHERE table_a.a = table_b.a AND table_a.a > 1000)";
  // clang-format on
};

class AwsQueryCompilerTest : public ::testing::Test {
 public:
  void SetUp() override {
    Aws::InitAPI(sdk_options_);
    client_ = std::make_shared<Client>();

    catalog_ = std::make_shared<RemoteCatalog>(client_, std::vector<std::string>(), kTpchDatabaseNameSF1000);
    if (!catalog_->TableExists(kTableNameLineitem)) {
      catalog_->CreateDatabase();
      TpchMockCatalog tpch_mock_catalog;
      catalog_->AddTable(kTableNameLineitem, tpch_mock_catalog.GetTableSchema("lineitem"), "skyrise-tpch-lineitem-data",
                         "s1000");
      Assert(catalog_->TableExists(kTableNameLineitem), "RemoteCatalog does not return lineitem table data.");
    }
  }

  void TearDown() override { Aws::ShutdownAPI(sdk_options_); }

 protected:
  std::shared_ptr<Client> client_;
  std::shared_ptr<RemoteCatalog> catalog_;

 private:
  Aws::SDKOptions sdk_options_;
};

TEST_F(QueryCompilerTest, CreateSingleStatement) {
  const std::string select_query_a = "SELECT * FROM table_a";
  auto query_compiler = QueryCompiler(select_query_a, catalog_);

  EXPECT_EQ(query_compiler.SqlStatementCount(), 1);
  EXPECT_EQ(query_compiler.SqlQueryString(), select_query_a);
  EXPECT_EQ(query_compiler.SqlStatementStrings().at(0), select_query_a);
}

TEST_F(QueryCompilerTest, CreateSingleStatementWithJoin) {
  auto query_compiler = QueryCompiler(join_query_, catalog_);

  EXPECT_EQ(query_compiler.SqlStatementCount(), 1);
  EXPECT_EQ(query_compiler.SqlQueryString(), join_query_);
  EXPECT_EQ(query_compiler.SqlStatementStrings().at(0), join_query_);
}

TEST_F(QueryCompilerTest, CreateMultiStatement) {
  const std::string multi_statement_dependant = "SELECT * FROM table_a; SELECT * FROM table_b;";
  auto query_compiler = QueryCompiler(multi_statement_dependant, catalog_);

  EXPECT_EQ(query_compiler.SqlStatementCount(), 2);
  EXPECT_EQ(query_compiler.SqlQueryString(), multi_statement_dependant);
  EXPECT_EQ(query_compiler.SqlStatementStrings().at(0), "SELECT * FROM table_a;");
  EXPECT_EQ(query_compiler.SqlStatementStrings().at(1), "SELECT * FROM table_b;");
}

TEST_F(QueryCompilerTest, GetParsedSQL) {
  auto query_compiler = QueryCompiler("SELECT * FROM table_a", catalog_);
  const auto& parsed_sql = query_compiler.ParsedSqlStatements().at(0);

  EXPECT_TRUE(parsed_sql->isValid());
  auto statements = parsed_sql->getStatements();
  EXPECT_EQ(statements.size(), 1u);
  EXPECT_EQ(statements.at(0)->type(), hsql::StatementType::kStmtSelect);
}

TEST_F(QueryCompilerTest, GetUnoptimizedLqp) {
  auto query_compiler = QueryCompiler("SELECT * FROM table_a", catalog_);

  const auto expected_lqp = StoredTableNode::Make("table_a", catalog_);

  EXPECT_LQP_EQ(query_compiler.GetOptimizedLqps().at(0), expected_lqp);
}

TEST_F(QueryCompilerTest, GetUnoptimizedLqpWithJoin) {
  auto query_compiler = QueryCompiler(join_query_, catalog_);

  const auto lqp = query_compiler.GetOptimizedLqps().at(0);
  EXPECT_TRUE(lqp);

  const auto stored_table_node_a = StoredTableNode::Make("table_a", catalog_);
  const auto a_a = stored_table_node_a->get_column("a");
  const auto a_b = stored_table_node_a->get_column("b");
  const auto stored_table_node_b = StoredTableNode::Make("table_b", catalog_);
  const auto b_a = stored_table_node_b->get_column("a");
  const auto b_b = stored_table_node_b->get_column("b");

  // clang-format off
  auto expected_lqp =
  AliasNode::Make(ExpressionVector_(a_a, a_b, b_b), std::vector<std::string>({"a", "b", "bb"}),
    ProjectionNode::Make(ExpressionVector_(a_a, a_b, b_b),
      PredicateNode::Make(Equals_(a_a, b_a),
        PredicateNode::Make(GreaterThan_(a_a, Value_(1000)),
          JoinNode::Make(JoinMode::kCross,
            stored_table_node_a,
            stored_table_node_b)))));

  // clang-format on
  EXPECT_LQP_EQ(lqp, expected_lqp);
}

TEST_F(QueryCompilerTest, Metrics) {
  const auto zero_duration = std::chrono::nanoseconds::zero();

  auto query_compiler = QueryCompiler("SELECT * FROM table_a", catalog_);
  const auto& metrics = query_compiler.Metrics();
  const auto& statement_metrics = metrics.statement_metrics.at(0);

  EXPECT_GT(metrics.parse_time_nanos, zero_duration);
  EXPECT_EQ(statement_metrics->sql_translation_duration, zero_duration);
  EXPECT_EQ(statement_metrics->lqp_optimization_duration, zero_duration);
  EXPECT_EQ(statement_metrics->lqp_translation_duration, zero_duration);
  EXPECT_EQ(statement_metrics->pqp_optimization_duration, zero_duration);
  EXPECT_EQ(statement_metrics->pqp_slicing_duration, zero_duration);

  // Run to get times
  EXPECT_EQ(query_compiler.GetPqpPipelines().size(), 1);

  EXPECT_GT(statement_metrics->sql_translation_duration, zero_duration);
  EXPECT_GT(statement_metrics->lqp_optimization_duration, zero_duration);
  EXPECT_GT(statement_metrics->lqp_translation_duration, zero_duration);
  EXPECT_GT(statement_metrics->pqp_optimization_duration, zero_duration);
  EXPECT_GT(statement_metrics->pqp_slicing_duration, zero_duration);
}

/**
 * // TODO(): Enable after implementing PlaceholderExpression
TEST_F(QueryCompilerTest, SqlTranslationInfo) {
  {
    auto query_compiler = QueryCompiler{"SELECT * FROM table_a"};
    auto translation_info = get_sql_pipeline_statements(sql_pipeline).at(0)->get_sql_translation_info();

    EXPECT_TRUE(translation_info.parameter_ids_of_value_placeholders.empty());
  }

  {
    auto query_compiler = QueryCompiler{"SELECT * FROM table_a WHERE a > ? AND b BETWEEN 5 AND ?"};
    auto translation_info = get_sql_pipeline_statements(sql_pipeline).at(0)->get_sql_translation_info();

    const auto parameters = std::vector<ParameterID>{ParameterID(0), ParameterID(1)};
    EXPECT_EQ(translation_info.parameter_ids_of_value_placeholders, parameters);
  }

  {
    auto query_compiler =
        QueryCompiler{
            "SELECT * FROM table_a t1 WHERE a > ? AND b = (SELECT MAX(b) FROM table_a t2 WHERE t2.a = t1.a AND b > ?)"};
    auto translation_info = get_sql_pipeline_statements(sql_pipeline).at(0)->get_sql_translation_info();

    const auto parameters = std::vector<ParameterID>{ParameterID(0), ParameterID(2)};
    EXPECT_EQ(translation_info.parameter_ids_of_value_placeholders, parameters);
  }
}
*/

TEST_F(QueryCompilerTest, TpchQ1) {
  auto q1 = TpchQueryGenerator::BuildDeterministicQuery(0);
  auto query_compiler = QueryCompiler(q1, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  EXPECT_EQ(query_compiler.GetPqpPipelines().size(), 2);
  //  VisualizePlans(query_compiler, "TpchQ1");
}

TEST_F(AwsQueryCompilerTest, TpchQ1) {
  auto q1 = TpchQueryGenerator::BuildDeterministicQuery(0);
  auto query_compiler = QueryCompiler(q1, catalog_);  // SF 1000
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  //  EXPECT_EQ(query_compiler.GetPqpPipelines().size(), xx);
  VisualizePlans(query_compiler, "Aws_TpchQ1");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ2) {  // misses hsql::kExprSelect
  auto q2 = TpchQueryGenerator::BuildDeterministicQuery(1);
  auto query_compiler = QueryCompiler(q2, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ2");
}

TEST_F(QueryCompilerTest, TpchQ3) {
  auto q3 = TpchQueryGenerator::BuildDeterministicQuery(2);
  auto query_compiler = QueryCompiler(q3, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ3");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ4) {  // misses hsql::kOpExists:
  auto q4 = TpchQueryGenerator::BuildDeterministicQuery(3);
  auto query_compiler = QueryCompiler(q4, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ4");
}

TEST_F(QueryCompilerTest, TpchQ5) {
  auto q5 = TpchQueryGenerator::BuildDeterministicQuery(4);
  auto query_compiler = QueryCompiler(q5, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ5");
}

TEST_F(QueryCompilerTest, TpchQ6) {
  auto q6 = TpchQueryGenerator::BuildDeterministicQuery(5);
  auto query_compiler = QueryCompiler(q6, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  EXPECT_EQ(query_compiler.GetPqpPipelines().size(), 2);
  VisualizePlans(query_compiler, "TpchQ6");
}

TEST_F(AwsQueryCompilerTest, TpchQ6) {
  auto q6 = TpchQueryGenerator::BuildDeterministicQuery(5);
  auto query_compiler = QueryCompiler(q6, catalog_);  // SF 1000
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  //  EXPECT_EQ(query_compiler.GetPqpPipelines().size(), xx);
  VisualizePlans(query_compiler, "Aws_TpchQ6");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ7) {  // misses FunctionExpression
  auto q7 = TpchQueryGenerator::BuildDeterministicQuery(6);
  auto query_compiler = QueryCompiler(q7, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ7");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ8) {  // misses FunctionExpression
  auto q8 = TpchQueryGenerator::BuildDeterministicQuery(7);
  auto query_compiler = QueryCompiler(q8, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ8");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ9) {  // misses FunctionExpression
  auto q9 = TpchQueryGenerator::BuildDeterministicQuery(8);
  auto query_compiler = QueryCompiler(q9, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ9");
}

TEST_F(QueryCompilerTest, TpchQ10) {
  auto q10 = TpchQueryGenerator::BuildDeterministicQuery(9);
  auto query_compiler = QueryCompiler(q10, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ10");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ11) {  // misses hsql::kExprSelect
  auto q11 = TpchQueryGenerator::BuildDeterministicQuery(10);
  auto query_compiler = QueryCompiler(q11, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ11");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ12) {  // misses CaseExpression
  auto q12 = TpchQueryGenerator::BuildDeterministicQuery(11);
  auto query_compiler = QueryCompiler(q12, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ12");
}

TEST_F(QueryCompilerTest, TpchQ13) {
  auto q13 = TpchQueryGenerator::BuildDeterministicQuery(12);
  auto query_compiler = QueryCompiler(q13, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ13");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ14) {  // misses CaseExpression
  auto q14 = TpchQueryGenerator::BuildDeterministicQuery(13);
  auto query_compiler = QueryCompiler(q14, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ14");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ15) {  // misses view multi-statement, view-functionality
  auto q15 = TpchQueryGenerator::BuildDeterministicQuery(14);
  auto query_compiler = QueryCompiler(q15, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ15");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ16) {  // misses `a IN (x, y, z)`
  auto q16 = TpchQueryGenerator::BuildDeterministicQuery(15);
  auto query_compiler = QueryCompiler(q16, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ16");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ17) {  // misses hsql::kExprSelect
  auto q17 = TpchQueryGenerator::BuildDeterministicQuery(16);
  auto query_compiler = QueryCompiler(q17, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ17");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ18) {  // misses a IN (SELECT ...)
  auto q18 = TpchQueryGenerator::BuildDeterministicQuery(17);
  auto query_compiler = QueryCompiler(q18, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ18");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ19) {  // misses `a IN (x, y, z)`
  auto q19 = TpchQueryGenerator::BuildDeterministicQuery(18);
  auto query_compiler = QueryCompiler(q19, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ19");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ20) {  // misses `a IN (SELECT ...)`
  auto q20 = TpchQueryGenerator::BuildDeterministicQuery(19);
  auto query_compiler = QueryCompiler(q20, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ20");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ21) {  // misses hsql::kOpExists
  auto q21 = TpchQueryGenerator::BuildDeterministicQuery(20);
  auto query_compiler = QueryCompiler(q21, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ21");
}

TEST_F(QueryCompilerTest, DISABLED_TpchQ22) {  // misses FunctionExpression
  auto q22 = TpchQueryGenerator::BuildDeterministicQuery(21);
  auto query_compiler = QueryCompiler(q22, catalog_);
  EXPECT_EQ(query_compiler.GetOptimizedLqps().size(), 1);
  EXPECT_EQ(query_compiler.GetOptimizedPqps().size(), 1);
  VisualizePlans(query_compiler, "TpchQ22");
}

}  // namespace skyrise
