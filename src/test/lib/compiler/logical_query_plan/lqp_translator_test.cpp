/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "compiler/logical_query_plan/lqp_translator.hpp"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/aggregate_node.hpp"
#include "compiler/logical_query_plan/change_meta_table_node.hpp"
#include "compiler/logical_query_plan/create_prepared_plan_node.hpp"
#include "compiler/logical_query_plan/create_table_node.hpp"
#include "compiler/logical_query_plan/drop_table_node.hpp"
#include "compiler/logical_query_plan/dummy_table_node.hpp"
#include "compiler/logical_query_plan/export_node.hpp"
#include "compiler/logical_query_plan/import_node.hpp"
#include "compiler/logical_query_plan/join_node.hpp"
#include "compiler/logical_query_plan/limit_node.hpp"
#include "compiler/logical_query_plan/predicate_node.hpp"
#include "compiler/logical_query_plan/projection_node.hpp"
#include "compiler/logical_query_plan/sort_node.hpp"
#include "compiler/logical_query_plan/static_table_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "compiler/logical_query_plan/union_node.hpp"
#include "compiler/logical_query_plan/validate_node.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/change_meta_table.hpp"
#include "operators/export.hpp"
#include "operators/get_table.hpp"
#include "operators/import.hpp"
#include "operators/index_scan.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/maintenance/create_prepared_plan.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/maintenance/drop_table.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "operators/union_positions.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/prepared_plan.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class LqpTranslatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    table_int_float = load_table("resources/test_data/tbl/int_float.tbl");
    table_int_string = load_table("resources/test_data/tbl/int_string.tbl");
    table_int_float2 = load_table("resources/test_data/tbl/int_float2.tbl");
    table_int_float5 = load_table("resources/test_data/tbl/int_float5.tbl");
    table_alias_name = load_table("resources/test_data/tbl/table_alias_name.tbl");

    Hyrise::get().storage_manager.add_table("table_int_float", table_int_float);
    Hyrise::get().storage_manager.add_table("table_int_string", table_int_string);
    Hyrise::get().storage_manager.add_table("table_int_float2", table_int_float2);
    Hyrise::get().storage_manager.add_table("table_int_float5", table_int_float5);
    Hyrise::get().storage_manager.add_table("table_alias_name", table_alias_name);
    Hyrise::get().storage_manager.add_table("int_float_chunked",
                                            load_table("resources/test_data/tbl/int_float.tbl", 1));
    ChunkEncoder::encode_all_chunks(Hyrise::get().storage_manager.get_table("int_float_chunked"));

    int_float_node = StoredTableNode::Make("table_int_float");
    int_float_a = int_float_node->get_column("a");
    int_float_b = int_float_node->get_column("b");

    int_string_node = StoredTableNode::Make("table_int_string");
    int_string_a = int_string_node->get_column("a");
    int_string_b = int_string_node->get_column("b");

    int_float2_node = StoredTableNode::Make("table_int_float2");
    int_float2_a = int_float2_node->get_column("a");
    int_float2_b = int_float2_node->get_column("b");

    int_float5_node = StoredTableNode::Make("table_int_float5");
    int_float5_a = int_float5_node->get_column("a");
    int_float5_d = int_float5_node->get_column("d");
  }

  std::shared_ptr<Table> table_int_float, table_int_float2, table_int_float5, table_int_string, table_alias_name;
  std::shared_ptr<StoredTableNode> int_float_node, int_float2_node, int_float5_node, int_string_node;
  std::shared_ptr<LqpColumnExpression> int_float_a, int_float_b, int_float2_a, int_float2_b, int_float5_a, int_float5_d,
      int_string_a, int_string_b;
};

TEST_F(LqpTranslatorTest, StoredTableNode) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *    SELECT a FROM table_int_float;
   */
  const auto pqp = LqpTranslator{}.translate_node(int_float_node);

  /**
   * Check PQP
   */
  const auto get_table_op = std::dynamic_pointer_cast<GetTable>(pqp);
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LqpTranslatorTest, ArithmeticExpression) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT a + b FROM table_int_float;
   */
  const auto a_plus_b_lqp =
      std::make_shared<ArithmeticExpression>(ArithmeticOperator::kAddition, int_float_a, int_float_b);
  const auto projection_expressions = std::vector<std::shared_ptr<AbstractExpression>>({a_plus_b_lqp});
  const auto projection_node = ProjectionNode::Make(projection_expressions, int_float_node);
  const auto pqp = LqpTranslator{}.translate_node(projection_node);

  /**
   * Check PQP
   */
  const auto projection_op = std::dynamic_pointer_cast<Projection>(pqp);
  ASSERT_TRUE(projection_op);
  ASSERT_EQ(projection_op->expressions.size(), 1u);
  const auto a_plus_b_pqp = std::dynamic_pointer_cast<ArithmeticExpression>(projection_op->expressions.at(0));
  ASSERT_TRUE(a_plus_b_pqp);
  EXPECT_EQ(a_plus_b_pqp->arithmetic_operator_, ArithmeticOperator::kAddition);

  const auto a_pqp = std::dynamic_pointer_cast<PQPColumnExpression>(a_plus_b_pqp->LeftOperand());
  ASSERT_TRUE(a_pqp);
  EXPECT_EQ(a_pqp->column_id, ColumnId{0});

  const auto b_pqp = std::dynamic_pointer_cast<PQPColumnExpression>(a_plus_b_pqp->RightOperand());
  ASSERT_TRUE(b_pqp);
  EXPECT_EQ(b_pqp->column_id, ColumnId{1});

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->LeftInput());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LqpTranslatorTest, PredicateNodeSimpleBinary) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float WHERE 5 > b;
   */
  const auto predicate_node = PredicateNode::Make(GreaterThan_(5, int_float_b), int_float_node);
  const auto pqp = LqpTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(pqp);
  const auto b = PQPColumnExpression::from_table(*table_int_float, ColumnId{1});
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(*table_scan_op->predicate(), *GreaterThan_(5, b));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->LeftInput());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LqpTranslatorTest, PredicateNodeLike) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_string WHERE b LIKE 'hello%';
   */
  const auto lqp = PredicateNode::Make(Like_(int_string_b, "hello%"), int_string_node);
  const auto pqp = LqpTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(pqp);
  const auto b = PQPColumnExpression::from_table(*table_int_string, ColumnId{1});
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(*table_scan_op->predicate(), *Like_(b, "hello%"));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->LeftInput());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_string");
}

TEST_F(LqpTranslatorTest, PredicateNodeUnary) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float WHERE b IS NOT NULL;
   */
  const auto predicate_node = PredicateNode::Make(IsNotNull_(int_float_b), int_float_node);
  const auto pqp = LqpTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(pqp);
  const auto b = PQPColumnExpression::from_table(*table_int_float, ColumnId{1});
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(*table_scan_op->predicate(), *IsNotNull_(b));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->LeftInput());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LqpTranslatorTest, PredicateNodeBetween) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float WHERE 5 BETWEEN a AND b;
   */
  const auto predicate_node = PredicateNode::Make(BetweenInclusive_(5, int_float_a, int_float_b), int_float_node);
  const auto pqp = LqpTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto a = PQPColumnExpression::from_table(*table_int_float, "a");
  const auto b = PQPColumnExpression::from_table(*table_int_float, "b");

  const auto between_scan_op = std::dynamic_pointer_cast<const TableScan>(pqp);
  ASSERT_TRUE(between_scan_op);
  EXPECT_EQ(*between_scan_op->predicate(), *BetweenInclusive_(5, a, b));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->LeftInput());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LqpTranslatorTest, SubqueryExpressionCorrelated) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT (SELECT MIN(a + int_float5.d + int_float5.a) FROM int_float), a FROM int_float5;
   */
  const auto parameter_a = correlated_parameter_(ParameterID{0}, int_float5_a);
  const auto parameter_d = correlated_parameter_(ParameterID{1}, int_float5_d);

  const auto a_plus_a_plus_d = Add_(int_float_a, Add_(parameter_a, parameter_d));

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::Make(ExpressionVector_(), ExpressionVector_(Min_(a_plus_a_plus_d)),
    ProjectionNode::Make(ExpressionVector_(a_plus_a_plus_d),
      int_float_node));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float5_a),
                                 std::make_pair(ParameterID{1}, int_float5_d));

  const auto lqp =
  ProjectionNode::Make(ExpressionVector_(subquery, int_float5_a), int_float5_node);
  // clang-format on

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  ASSERT_EQ(pqp->type(), OperatorType::Projection);
  ASSERT_TRUE(pqp->LeftInput());
  ASSERT_EQ(pqp->LeftInput()->type(), OperatorType::GetTable);

  const auto projection = std::static_pointer_cast<const Projection>(pqp);
  ASSERT_EQ(projection->expressions.size(), 2u);

  const auto expression_a = std::dynamic_pointer_cast<PQPSubqueryExpression>(projection->expressions.at(0));
  ASSERT_TRUE(expression_a);
  ASSERT_EQ(expression_a->parameters.size(), 2u);
  ASSERT_EQ(expression_a->parameters.at(0).first, ParameterID{0});
  ASSERT_EQ(expression_a->parameters.at(0).second, ColumnId{0});
  ASSERT_EQ(expression_a->parameters.at(1).first, ParameterID{1});
  ASSERT_EQ(expression_a->parameters.at(1).second, ColumnId{1});

  ASSERT_EQ(expression_a->pqp->type(), OperatorType::Aggregate);

  const auto expression_b = std::dynamic_pointer_cast<PQPColumnExpression>(projection->expressions.at(1));
  ASSERT_TRUE(expression_b);
}

TEST_F(LqpTranslatorTest, Sort) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT a, b FROM int_float ORDER BY a, a + b DESC, b ASC
   */

  const auto sort_modes = std::vector<SortMode>({SortMode::kAscending, SortMode::kDescending});

  // clang-format off
  const auto lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_a, int_float_b),
    SortNode::Make(ExpressionVector_(int_float_a, Add_(int_float_a, int_float_b)), sort_modes,
      ProjectionNode::Make(ExpressionVector_(Add_(int_float_a, int_float_b), int_float_a, int_float_b),
        int_float_node)));

  // clang-format on
  const auto pqp = LqpTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto projection_a = std::dynamic_pointer_cast<const Projection>(pqp);
  ASSERT_TRUE(projection_a);

  const auto sort = std::dynamic_pointer_cast<const Sort>(pqp->LeftInput());
  ASSERT_TRUE(sort);

  EXPECT_EQ(sort->sort_definitions().at(0).column, ColumnId{1});
  EXPECT_EQ(sort->sort_definitions().at(0).sort_mode, SortMode::kAscending);

  EXPECT_EQ(sort->sort_definitions().at(1).column, ColumnId{0});
  EXPECT_EQ(sort->sort_definitions().at(1).sort_mode, SortMode::kDescending);

  const auto projection_b = std::dynamic_pointer_cast<const Projection>(sort->LeftInput());
  ASSERT_TRUE(projection_b);

  const auto get_table = std::dynamic_pointer_cast<const GetTable>(projection_b->LeftInput());
  ASSERT_TRUE(get_table);
}

TEST_F(LqpTranslatorTest, LimitLiteral) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float LIMIT 1337
   */
  const auto lqp = LimitNode::Make(Value_(static_cast<int64_t>(1337)), int_float_node);
  const auto pqp = LqpTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto limit = std::dynamic_pointer_cast<Limit>(pqp);
  ASSERT_TRUE(limit);
  const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(limit->row_count_expression());
  ASSERT_TRUE(value_expression);
  ASSERT_EQ(value_expression->value, AllTypeVariant(static_cast<int64_t>(1337)));

  const auto get_table = std::dynamic_pointer_cast<const GetTable>(limit->LeftInput());
  ASSERT_TRUE(get_table);
  EXPECT_EQ(get_table->table_name(), "table_int_float");
}

TEST_F(LqpTranslatorTest, PredicateNodeUnaryScan) {
  /**
   * Build LQP and translate to PQP
   */
  auto predicate_node = PredicateNode::Make(Equals_(int_float_b, 42), int_float_node);
  const auto op = LqpTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  const auto b = PQPColumnExpression::from_table(*table_int_float, "b");
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(*table_scan_op->predicate(), *Equals_(b, 42));
}

TEST_F(LqpTranslatorTest, PredicateNodeBetweenScan) {
  /**
   * Build LQP and translate to PQP
   */
  auto predicate_node = PredicateNode::Make(BetweenInclusive_(int_float_a, 42, 1337), int_float_node);
  const auto op = LqpTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op);

  const auto a = PQPColumnExpression::from_table(*table_int_float, "a");
  EXPECT_EQ(*table_scan_op->predicate(), *BetweenInclusive_(a, 42, 1337));
}

// Tests accessing the original LQP node after translation.
TEST_F(LqpTranslatorTest, LqpNodeAccess) {
  auto predicate_node = PredicateNode::Make(BetweenInclusive_(int_float_a, 42, 1337), int_float_node);
  auto validate_node = ValidateNode::Make(predicate_node);
  auto join_node = JoinNode::Make(JoinMode::kInner, Equals_(int_float_a, int_float2_a), validate_node, int_float2_node);
  auto aggregate_node = AggregateNode::Make(ExpressionVector_(int_float_a, int_float_b),
                                            ExpressionVector_(Sum_(int_float_a), Sum_(int_float_b)), join_node);
  const auto op = LqpTranslator{}.translate_node(aggregate_node);

  {
    const auto lqp_node = op->lqp_node;
    const auto recovered_node = std::dynamic_pointer_cast<const AggregateNode>(lqp_node);
    EXPECT_EQ(recovered_node, aggregate_node);
  }
  {
    const auto lqp_node = op->LeftInput()->lqp_node;
    const auto recovered_node = std::dynamic_pointer_cast<const JoinNode>(lqp_node);
    EXPECT_EQ(recovered_node, join_node);
  }
  {
    const auto lqp_node_left = op->LeftInput()->LeftInput()->lqp_node;
    const auto recovered_node_left = std::dynamic_pointer_cast<const ValidateNode>(lqp_node_left);
    EXPECT_EQ(recovered_node_left, validate_node);
    const auto lqp_node_right = op->LeftInput()->RightInput()->lqp_node;
    const auto recovered_node_right = std::dynamic_pointer_cast<const StoredTableNode>(lqp_node_right);
    EXPECT_EQ(recovered_node_right, int_float2_node);
  }
  {
    const auto lqp_node = op->LeftInput()->LeftInput()->LeftInput()->lqp_node;
    const auto recovered_node = std::dynamic_pointer_cast<const PredicateNode>(lqp_node);
    EXPECT_EQ(recovered_node, predicate_node);
  }
  {
    const auto lqp_node = op->LeftInput()->LeftInput()->LeftInput()->LeftInput()->lqp_node;
    const auto recovered_node = std::dynamic_pointer_cast<const StoredTableNode>(lqp_node);
    EXPECT_EQ(recovered_node, int_float_node);
  }
}

// Check if the LQP that is referenced in the PQP is really cleaned up. This test is intended to check that no cyclic
// references are accidentally introduced a later point in time.
TEST_F(LqpTranslatorTest, PQPReferencedLqpNodeCleanUp) {
  std::weak_ptr<const AbstractLqpNode> lqp_node;
  {
    auto pipeline_statement = SQLPipelineBuilder{"SELECT a FROM table_int_float WHERE a < 42"}.create_pipeline();
    const auto pqp = pipeline_statement.get_physical_plans().at(0);
    lqp_node = pqp->lqp_node;
    EXPECT_FALSE(lqp_node.expired());
  }
  EXPECT_TRUE(lqp_node.expired());
}

TEST_F(LqpTranslatorTest, PredicateNodeIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::Make("int_float_chunked");

  const auto table = Hyrise::get().storage_manager.get_table("int_float_chunked");
  std::vector<ColumnId> index_column_ids = {ColumnId{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node = PredicateNode::Make(Equals_(stored_table_node->get_column("b"), 42));
  predicate_node->SetLeftInput(stored_table_node);
  predicate_node->scan_type = ScanType::IndexScan;
  const auto op = LqpTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto union_op = std::dynamic_pointer_cast<UnionAll>(op);
  ASSERT_TRUE(union_op);

  const auto index_scan_op = std::dynamic_pointer_cast<const IndexScan>(op->LeftInput());
  ASSERT_TRUE(index_scan_op);
  EXPECT_EQ(index_scan_op->included_chunk_ids, index_chunk_ids);

  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(op->RightInput());
  const auto b = PQPColumnExpression::from_table(*table, "b");
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->excluded_chunk_ids, index_chunk_ids);
  EXPECT_EQ(*table_scan_op->predicate(), *Equals_(b, 42));

  // Check the setting of LQP nodes for index scans
  EXPECT_EQ(union_op->lqp_node, predicate_node);
  EXPECT_EQ(index_scan_op->lqp_node, predicate_node);
  EXPECT_EQ(table_scan_op->lqp_node, predicate_node);
}

TEST_F(LqpTranslatorTest, PredicateNodePrunedIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::Make("int_float_chunked");

  const auto table = Hyrise::get().storage_manager.get_table("int_float_chunked");
  auto index_column_ids = std::vector{ColumnId{1}};
  auto index_chunk_ids = std::vector{ChunkID{0}, ChunkID{2}};
  auto pruned_chunk_ids = std::vector{ChunkID{0}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  stored_table_node->set_pruned_chunk_ids(pruned_chunk_ids);
  auto predicate_node = PredicateNode::Make(Equals_(stored_table_node->get_column("b"), 42));
  predicate_node->SetLeftInput(stored_table_node);
  predicate_node->scan_type = ScanType::IndexScan;
  const auto op = LqpTranslator{}.translate_node(predicate_node);

  // As the vector of indexed chunks contains the chunk ids {0, 2} and the vector of pruned chunks
  // contains the chunk id {0}, the first indexed chunk is pruned. Correspondingly, the ids of the
  // indexed chunks have to be adapted, so that the indexed chunk with the id 2 now has the id 1.
  std::vector<ChunkID> index_scan_chunk_ids = {ChunkID{1}};

  /**
   * Check PQP
   */
  const auto union_op = std::dynamic_pointer_cast<UnionAll>(op);
  ASSERT_TRUE(union_op);

  const auto index_scan_op = std::dynamic_pointer_cast<const IndexScan>(op->LeftInput());
  ASSERT_TRUE(index_scan_op);
  EXPECT_EQ(index_scan_op->included_chunk_ids, index_scan_chunk_ids);

  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(op->RightInput());
  const auto b = PQPColumnExpression::from_table(*table, "b");
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->excluded_chunk_ids, index_scan_chunk_ids);
  EXPECT_EQ(*table_scan_op->predicate(), *Equals_(b, 42));

  // Check the setting of LQP nodes for index scans
  EXPECT_EQ(union_op->lqp_node, predicate_node);
  EXPECT_EQ(index_scan_op->lqp_node, predicate_node);
  EXPECT_EQ(table_scan_op->lqp_node, predicate_node);
}

TEST_F(LqpTranslatorTest, PredicateNodeBinaryIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::Make("int_float_chunked");

  const auto table = Hyrise::get().storage_manager.get_table("int_float_chunked");
  std::vector<ColumnId> index_column_ids = {ColumnId{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node = PredicateNode::Make(BetweenInclusive_(stored_table_node->get_column("b"), 42, 1337));
  predicate_node->SetLeftInput(stored_table_node);
  predicate_node->scan_type = ScanType::IndexScan;
  const auto op = LqpTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto union_op = std::dynamic_pointer_cast<UnionAll>(op);
  ASSERT_TRUE(union_op);

  const auto index_scan_op = std::dynamic_pointer_cast<const IndexScan>(op->LeftInput());
  ASSERT_TRUE(index_scan_op);
  EXPECT_EQ(index_scan_op->included_chunk_ids, index_chunk_ids);
  EXPECT_EQ(index_scan_op->LeftInput()->type(), OperatorType::GetTable);

  const auto b = PQPColumnExpression::from_table(*table, "b");
  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(op->RightInput());
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->excluded_chunk_ids, index_chunk_ids);
  EXPECT_EQ(*table_scan_op->predicate(), *BetweenInclusive_(b, 42, 1337));
}

TEST_F(LqpTranslatorTest, PredicateNodeIndexScanFailsWhenNotApplicable) {
  if (!SKYRISE_DEBUG) GTEST_SKIP();

  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::Make("int_float_chunked");

  const auto table = Hyrise::get().storage_manager.get_table("int_float_chunked");
  std::vector<ColumnId> index_column_ids = {ColumnId{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node = PredicateNode::Make(Equals_(stored_table_node->get_column("b"), 42));
  predicate_node->SetLeftInput(stored_table_node);
  auto predicate_node2 = PredicateNode::Make(LessThan_(stored_table_node->get_column("a"), 42));
  predicate_node2->SetLeftInput(predicate_node);

  // The optimizer should not set this ScanType in this situation
  predicate_node2->scan_type = ScanType::IndexScan;
  EXPECT_THROW(LqpTranslator{}.translate_node(predicate_node2), std::logic_error);
}

TEST_F(LqpTranslatorTest, ProjectionNode) {
  /**
   * Build LQP and translate to PQP
   */
  auto projection_node = ProjectionNode::Make(ExpressionVector_(int_float_a), int_float_node);
  const auto op = LqpTranslator{}.translate_node(projection_node);

  /**
   * Check PQP
   */
  const auto projection_op = std::dynamic_pointer_cast<Projection>(op);
  ASSERT_TRUE(projection_op);
  EXPECT_EQ(projection_op->expressions.size(), 1u);
  EXPECT_EQ(*projection_op->expressions[0], *PQPColumnExpression::from_table(*table_int_float, "a"));
}

TEST_F(LqpTranslatorTest, JoinNodeToJoinHash) {
  /**
   * Build LQP and translate to PQP
   */
  auto join_node =
      JoinNode::Make(JoinMode::kInner, Equals_(int_float2_b, int_float_b), int_float_node, int_float2_node);
  const auto op = LqpTranslator{}.translate_node(join_node);

  /**
   * Check PQP - for a inner-equi join, JoinHash should be used.
   */
  const auto join_op = std::dynamic_pointer_cast<JoinHash>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->primary_predicate().column_ids, ColumnIdPair(ColumnId{1}, ColumnId{1}));
  EXPECT_EQ(join_op->primary_predicate() predicate_condition_, PredicateCondition::kEquals);
  EXPECT_EQ(join_op->mode(), JoinMode::kInner);
}

TEST_F(LqpTranslatorTest, JoinNodeToJoinSortMerge) {
  /**
   * Build LQP and translate to PQP
   */
  auto join_node =
      JoinNode::Make(JoinMode::kInner, LessThan_(int_float_b, int_float2_b), int_float_node, int_float2_node);
  const auto op = LqpTranslator{}.translate_node(join_node);

  /**
   * Check PQP - JoinHash doesn't support non-equi joins, thus we fall back to JoinSortMerge
   */
  const auto join_op = std::dynamic_pointer_cast<JoinSortMerge>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->primary_predicate().column_ids, ColumnIdPair(ColumnId{1}, ColumnId{1}));
  EXPECT_EQ(join_op->primary_predicate() predicate_condition_, PredicateCondition::kLessThan);
  EXPECT_EQ(join_op->mode(), JoinMode::kInner);
}

TEST_F(LqpTranslatorTest, JoinNodeToJoinNestedLoop) {
  /**
   * Build LQP and translate to PQP
   */
  auto join_node =
      JoinNode::Make(JoinMode::kInner, LessThan_(int_float_a, int_float2_b), int_float_node, int_float2_node);
  const auto op = LqpTranslator{}.translate_node(join_node);

  /**
   * Check PQP - Neither JoinHash nor JoinSortMerge support non-equi joins on different column types. So we fall back to
   * JoinNestedLoop.
   */
  const auto join_op = std::dynamic_pointer_cast<JoinNestedLoop>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->primary_predicate().column_ids, ColumnIdPair(ColumnId{0}, ColumnId{1}));
  EXPECT_EQ(join_op->primary_predicate() predicate_condition_, PredicateCondition::kLessThan);
  EXPECT_EQ(join_op->mode(), JoinMode::kInner);
}

TEST_F(LqpTranslatorTest, AggregateNodeSimple) {
  /**
   * Build LQP and translate to PQP
   */
  // clang-format off
  const auto lqp =
  AggregateNode::Make(ExpressionVector_(int_float_a), ExpressionVector_(Sum_(Add_(int_float_b, int_float_a)), CountStarLqp_(int_float_node)),  // NOLINT
    ProjectionNode::Make(ExpressionVector_(int_float_b, int_float_a, Add_(int_float_b, int_float_a)),
      int_float_node));
  // clang-format on
  const auto op = LqpTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto aggregate_op = std::dynamic_pointer_cast<AggregateHash>(op);
  ASSERT_TRUE(aggregate_op);
  ASSERT_EQ(aggregate_op->aggregates().size(), 2u);
  ASSERT_EQ(aggregate_op->groupby_column_ids().size(), 1u);
  EXPECT_EQ(aggregate_op->groupby_column_ids().at(0), ColumnId{1});

  const auto sum = aggregate_op->aggregates()[0];
  EXPECT_EQ(*sum, *Sum_(PqpColumn_(ColumnId{2}, DataType::kFloat, false, "b + a")));

  const auto count = aggregate_op->aggregates()[1];
  EXPECT_EQ(*count, *Count_(PqpColumn_(kInvalidColumnId, DataType::kLong, false, "*")));
}

TEST_F(LqpTranslatorTest, JoinAndPredicates) {
  /**
   * Build LQP and translate to PQP
   */
  auto predicate_node_left = PredicateNode::Make(Equals_(int_float_a, 42), int_float_node);
  auto predicate_node_right = PredicateNode::Make(GreaterThan_(int_float2_b, 30.0), int_float2_node);

  auto join_node = JoinNode::Make(JoinMode::kInner, Equals_(int_float_a, int_float2_a));
  join_node->SetLeftInput(predicate_node_left);
  join_node->SetRightInput(predicate_node_right);

  const auto op = LqpTranslator{}.translate_node(join_node);

  /**
   * Check PQP
   */
  const auto a = PQPColumnExpression::from_table(*table_int_float, "a");
  const auto b = PQPColumnExpression::from_table(*table_int_float2, "b");

  const auto join_op = std::dynamic_pointer_cast<const JoinHash>(op);
  ASSERT_TRUE(join_op);

  const auto predicate_op_left = std::dynamic_pointer_cast<const TableScan>(join_op->LeftInput());
  ASSERT_TRUE(predicate_op_left);
  ASSERT_EQ(*predicate_op_left->predicate(), *Equals_(a, 42));

  const auto predicate_op_right = std::dynamic_pointer_cast<const TableScan>(join_op->RightInput());
  ASSERT_TRUE(predicate_op_right);
  ASSERT_EQ(*predicate_op_right->predicate(), *GreaterThan_(b, 30.0));

  const auto get_table_op_left = std::dynamic_pointer_cast<const GetTable>(predicate_op_left->LeftInput());
  ASSERT_TRUE(get_table_op_left);
  EXPECT_EQ(get_table_op_left->table_name(), "table_int_float");

  const auto get_table_op_right = std::dynamic_pointer_cast<const GetTable>(predicate_op_right->LeftInput());
  ASSERT_TRUE(get_table_op_right);
  EXPECT_EQ(get_table_op_right->table_name(), "table_int_float2");
}

TEST_F(LqpTranslatorTest, LimitNode) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::Make("table_int_float");

  auto limit_node = LimitNode::Make(Value_(2));
  limit_node->SetLeftInput(stored_table_node);

  /**
   * Check PQP
   */
  const auto op = LqpTranslator{}.translate_node(limit_node);
  const auto limit_op = std::dynamic_pointer_cast<Limit>(op);
  ASSERT_TRUE(limit_op);
  EXPECT_EQ(*limit_op->row_count_expression(), *Value_(2));
}

TEST_F(LqpTranslatorTest, DiamondShapeSimple) {
  /**
   * Test that
   *
   *    _____union____
   *   /              \
   *  predicate_a     predicate_b
   *  \                /
   *   \__predicate_c_/
   *          |
   *     table_int_float2
   *
   * has a diamond shape in the PQP as well. If it wouldn't have it might look like this:
   *
   *    _____union____
   *   /              \
   *  predicate_a     predicate_b
   *      |             |
   *  predicate_c(1)  predicate_c(2)
   *      |             |
   * table_int_float2 table_int_float2
   *
   * which is still semantically correct, but would mean predicate_c gets executed twice
   */

  auto predicate_node_a = PredicateNode::Make(Equals_(int_float2_a, 3));
  auto predicate_node_b = PredicateNode::Make(Equals_(int_float2_a, 4));
  auto predicate_node_c = PredicateNode::Make(Equals_(int_float2_b, 5));
  auto union_node = UnionNode::Make(SetOperationMode::kAll);
  const auto& lqp = union_node;

  union_node->SetLeftInput(predicate_node_a);
  union_node->SetRightInput(predicate_node_b);
  predicate_node_a->SetLeftInput(predicate_node_c);
  predicate_node_b->SetLeftInput(predicate_node_c);
  predicate_node_c->SetLeftInput(int_float2_node);

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  ASSERT_NE(pqp, nullptr);
  ASSERT_NE(pqp->LeftInput(), nullptr);
  ASSERT_NE(pqp->RightInput(), nullptr);
  ASSERT_NE(pqp->LeftInput()->LeftInput(), nullptr);
  ASSERT_NE(pqp->RightInput()->LeftInput(), nullptr);
  EXPECT_EQ(pqp->LeftInput()->LeftInput(), pqp->RightInput()->LeftInput());
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput(), pqp->RightInput()->LeftInput()->LeftInput());
}

TEST_F(LqpTranslatorTest, DiamondShapeIncludeUncorrelatedSubqueries) {
  // Tests that PQP parts that are shared between an uncorrelated subquery and the outer plan are deduplicated.

  // Prepare uncorrelated subquery that uses int_float_a from root LQP
  // clang-format off
  auto subquery_lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_b),
    JoinNode::Make(JoinMode::kInner, Equals_(int_float_b, int_float2_b),
      int_float_node,
      int_float2_node));
  auto lqp_subquery_expression = lqp_subquery_(subquery_lqp);

  auto root_lqp =
  PredicateNode::Make(GreaterThan_(int_float_a, lqp_subquery_expression),
    int_float_node);
  // clang-format on

  const auto pqp = LqpTranslator{}.translate_node(root_lqp);

  // Get operators of root PQP
  ASSERT_EQ(pqp->type(), OperatorType::TableScan);
  const auto table_scan = std::static_pointer_cast<const TableScan>(pqp);
  ASSERT_EQ(table_scan->LeftInput()->type(), OperatorType::GetTable);

  const auto get_table_int_float = std::static_pointer_cast<const GetTable>(pqp->LeftInput());

  // Get operators of subquery PQP
  const auto greater_than_predicate =
      std::dynamic_pointer_cast<const BinaryPredicateExpression>(table_scan->predicate());
  ASSERT_TRUE(greater_than_predicate &&
              greater_than_predicate->predicate_condition_ == PredicateCondition::kGreaterThan);
  ASSERT_EQ(greater_than_predicate->LeftOperand()->type_, ExpressionType::kPQPColumn);
  ASSERT_EQ(greater_than_predicate->RightOperand()->type_, ExpressionType::PQPSubquery);

  const auto pqp_subquery_expression =
      std::static_pointer_cast<const PQPSubqueryExpression>(greater_than_predicate->RightOperand());
  ASSERT_FALSE(pqp_subquery_expression->is_correlated());
  ASSERT_EQ(pqp_subquery_expression->pqp->type(), OperatorType::Projection);

  const auto subquery_projection = std::static_pointer_cast<const Projection>(pqp_subquery_expression->pqp);
  ASSERT_EQ(subquery_projection->LeftInput()->type(), OperatorType::JoinHash);

  const auto subquery_join = std::static_pointer_cast<const JoinHash>(subquery_projection->LeftInput());
  ASSERT_EQ(subquery_join->LeftInput()->type(), OperatorType::GetTable);
  ASSERT_EQ(subquery_join->RightInput()->type(), OperatorType::GetTable);

  // Compare addresses to check if the uncorrelated PQP subquery reuses the GetTable instance from its owning PQP.
  EXPECT_EQ(subquery_join->LeftInput(), get_table_int_float);
}

TEST_F(LqpTranslatorTest, DiamondShapeExcludeCorrelatedSubqueries) {
  // Tests that PQP parts that are shared between a correlated subquery and the outer plan are NOT deduplicated.

  // Prepare correlated subquery
  // clang-format off
  const auto correlated_parameter_a = correlated_parameter_(ParameterID{0}, int_float_a);
  auto subquery_lqp =
  ProjectionNode::Make(ExpressionVector_(Sub_(int_float_b, correlated_parameter_a)),
    JoinNode::Make(JoinMode::kInner, Equals_(int_float_b, int_float2_b),
      int_float_node,
      int_float2_node));
  auto lqp_subquery_expression = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float_a));

  auto root_lqp =
  PredicateNode::Make(GreaterThan_(int_float_a, lqp_subquery_expression),
    int_float_node);
  // clang-format on

  const auto pqp = LqpTranslator{}.translate_node(root_lqp);

  // Get operators of root PQP
  ASSERT_EQ(pqp->type(), OperatorType::TableScan);
  const auto table_scan = std::static_pointer_cast<const TableScan>(pqp);
  ASSERT_EQ(table_scan->LeftInput()->type(), OperatorType::GetTable);

  const auto get_table_int_float = std::static_pointer_cast<const GetTable>(pqp->LeftInput());

  // Get operators of subquery PQP
  const auto greater_than_predicate =
      std::dynamic_pointer_cast<const BinaryPredicateExpression>(table_scan->predicate());
  ASSERT_TRUE(greater_than_predicate &&
              greater_than_predicate->predicate_condition_ == PredicateCondition::kGreaterThan);
  ASSERT_EQ(greater_than_predicate->LeftOperand()->type_, ExpressionType::kPQPColumn);
  ASSERT_EQ(greater_than_predicate->RightOperand()->type_, ExpressionType::PQPSubquery);

  const auto pqp_subquery_expression =
      std::static_pointer_cast<const PQPSubqueryExpression>(greater_than_predicate->RightOperand());
  ASSERT_TRUE(pqp_subquery_expression->is_correlated());
  ASSERT_EQ(pqp_subquery_expression->pqp->type(), OperatorType::Projection);

  const auto subquery_projection = std::static_pointer_cast<const Projection>(pqp_subquery_expression->pqp);
  ASSERT_EQ(subquery_projection->LeftInput()->type(), OperatorType::JoinHash);

  const auto subquery_join = std::static_pointer_cast<const JoinHash>(subquery_projection->LeftInput());
  ASSERT_EQ(subquery_join->LeftInput()->type(), OperatorType::GetTable);
  ASSERT_EQ(subquery_join->RightInput()->type(), OperatorType::GetTable);

  // Compare addresses to check if the correlated PQP subquery uses a different GetTable instance than its owning PQP.
  EXPECT_NE(subquery_join->LeftInput(), get_table_int_float);
}

TEST_F(LqpTranslatorTest, ReusingPQPSelfJoin) {
  /**
   * Test that LQP:
   *
   *               Projection b, b
   *                      |
   *           ______Cross Join______
   *          /                      \
   *     Predicate                Predicate
   *     b = 456.7f               b = 457.7f
   *         |                        |
   *     Predicate                Predicate
   *     a = 12345                a = 12345
   *         |                        |
   *    StoredTable              StoredTable
   *  table_int_float2         table_int_float2
   *
   * is translated to PQP:
   *
   *           Projection b, b
   *                  |
   *       ________Product_______
   *      /                      \
   *  TableScan               TableScan
   *  b = 456.7f              b = 457.7f
   *     \________TableScan______/
   *              a = 12345
   *                  |
   *              GetTable
   *           table_int_float2
   *
   */

  auto int_float_node_1 = StoredTableNode::Make("table_int_float2");
  auto int_float_a_1 = int_float_node_1->get_column("a");
  auto int_float_b_1 = int_float_node_1->get_column("b");

  auto int_float_node_2 = StoredTableNode::Make("table_int_float2");
  auto int_float_a_2 = int_float_node_2->get_column("a");
  auto int_float_b_2 = int_float_node_2->get_column("b");

  // clang-format off
  const auto lqp =
  ProjectionNode::Make(ExpressionVector_(int_float_b_1, int_float_b_2),
    JoinNode::Make(JoinMode::kCross,
      PredicateNode::Make(Equals_(int_float_b_1, 456.7f),
        PredicateNode::Make(Equals_(int_float_a_1, 12345),
          int_float_node_1)),
      PredicateNode::Make(Equals_(int_float_b_2, 457.7f),
        PredicateNode::Make(Equals_(int_float_a_2, 12345),
          int_float_node_2))));
  // clang-format on

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  const auto projection = std::dynamic_pointer_cast<const Projection>(pqp);
  ASSERT_NE(projection, nullptr);

  const auto product = std::dynamic_pointer_cast<const Product>(projection->LeftInput());
  ASSERT_NE(product, nullptr);

  const auto table_scan_b_1 = std::dynamic_pointer_cast<const TableScan>(product->LeftInput());
  const auto table_scan_b_2 = std::dynamic_pointer_cast<const TableScan>(product->RightInput());
  ASSERT_NE(table_scan_b_1, nullptr);
  ASSERT_NE(table_scan_b_2, nullptr);
  ASSERT_NE(table_scan_b_1, table_scan_b_2);

  const auto table_scan_a_1 = std::dynamic_pointer_cast<const TableScan>(table_scan_b_1->LeftInput());
  const auto table_scan_a_2 = std::dynamic_pointer_cast<const TableScan>(table_scan_b_2->LeftInput());
  ASSERT_NE(table_scan_a_1, nullptr);
  ASSERT_NE(table_scan_a_2, nullptr);
  ASSERT_EQ(table_scan_a_1, table_scan_a_2);

  const auto get_table = std::dynamic_pointer_cast<const GetTable>(table_scan_a_1->LeftInput());
  ASSERT_NE(get_table, nullptr);

  ASSERT_EQ(get_table->LeftInput(), nullptr);
}

TEST_F(LqpTranslatorTest, ReuseInputExpressions) {
  // If the result of a (sub)expression is available in an input column, the expression should not be redundantly
  // evaluated

  // clang-format off
  const auto lqp =
  PredicateNode::Make(GreaterThan_(Add_(Add_(int_float_a, int_float_b), 3), 2),
    ProjectionNode::Make(ExpressionVector_(Add_(Add_(int_float_a, int_float_b), 3)),
      ProjectionNode::Make(ExpressionVector_(5, Add_(int_float_a, int_float_b)),
        int_float_node)));
  // clang-format on

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  ASSERT_NE(pqp, nullptr);
  ASSERT_NE(pqp->LeftInput(), nullptr);
  ASSERT_NE(pqp->LeftInput()->LeftInput(), nullptr);

  const auto table_scan = std::dynamic_pointer_cast<const TableScan>(pqp);
  const auto projection_a = std::dynamic_pointer_cast<const Projection>(pqp->LeftInput());
  const auto projection_b = std::dynamic_pointer_cast<const Projection>(pqp->LeftInput()->LeftInput());

  ASSERT_NE(table_scan, nullptr);
  ASSERT_NE(projection_a, nullptr);
  ASSERT_NE(projection_b, nullptr);

  const auto a_plus_b_in_temporary_column = PqpColumn_(ColumnId{1}, DataType::kFloat, false, "a + b");
  const auto scan_column_expression =
      std::dynamic_pointer_cast<PQPColumnExpression>(table_scan->predicate()->arguments_.at(0));

  ASSERT_TRUE(scan_column_expression);
  EXPECT_EQ(scan_column_expression->column_id, ColumnId{0});
  EXPECT_EQ(*projection_a->expressions.at(0), *Add_(a_plus_b_in_temporary_column, 3));
}

TEST_F(LqpTranslatorTest, ReuseSubqueryExpression) {
  // Test that subquery expressions whose result is available in an output column of the input operator are not
  // evaluated redundantly

  // clang-format off
  const auto subquery =
  ProjectionNode::Make(ExpressionVector_(Add_(1, 2)),
    DummyTableNode::Make());

  const auto subquery_a = lqp_subquery_(subquery);
  const auto subquery_b = lqp_subquery_(subquery);

  const auto lqp =
  ProjectionNode::Make(ExpressionVector_(Add_(subquery_a, 3)),
    ProjectionNode::Make(ExpressionVector_(5, subquery_b),
      int_float_node));
  // clang-format on

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  ASSERT_NE(pqp, nullptr);
  ASSERT_NE(pqp->LeftInput(), nullptr);

  const auto projection_a = std::dynamic_pointer_cast<const Projection>(pqp);
  const auto projection_b = std::dynamic_pointer_cast<const Projection>(pqp->LeftInput());

  ASSERT_NE(projection_a, nullptr);
  ASSERT_NE(projection_b, nullptr);

  // As subquery columns without an explicit alias get the LQP/PQP address as their name, we need to retrieve it first.
  const auto column_name = subquery_a->AsColumnName();
  const auto subquery_in_temporary_column = PqpColumn_(ColumnId{1}, DataType::kInt, false, column_name);

  EXPECT_EQ(*projection_a->expressions.at(0), *Add_(subquery_in_temporary_column, 3));
}

TEST_F(LqpTranslatorTest, CreateTable) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::kInt, false);
  column_definitions.emplace_back("b", DataType::kFloat, true);

  const auto lqp =
      CreateTableNode::Make("t", false, StaticTableNode::Make(Table::create_dummy_table(column_definitions)));

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  EXPECT_EQ(pqp->type(), OperatorType::CreateTable);

  const auto create_table = std::dynamic_pointer_cast<CreateTable>(pqp);
  EXPECT_EQ(create_table->table_name, "t");

  // CreateTable input must be executed to enable access to column definitions
  create_table->mutable_LeftInput()->execute();
  EXPECT_EQ(create_table->column_definitions(), column_definitions);
}

TEST_F(LqpTranslatorTest, StaticTable) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::kInt, false);
  column_definitions.emplace_back("b", DataType::kFloat, true);

  const auto dummy_table = Table::create_dummy_table(column_definitions);

  const auto lqp = StaticTableNode::Make(dummy_table);

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  EXPECT_EQ(pqp->type(), OperatorType::TableWrapper);

  const auto table_wrapper = std::dynamic_pointer_cast<TableWrapper>(pqp);
  EXPECT_EQ(table_wrapper->table, dummy_table);
}

TEST_F(LqpTranslatorTest, DropTable) {
  const auto lqp = DropTableNode::Make("t", false);

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  EXPECT_EQ(pqp->type(), OperatorType::DropTable);
  EXPECT_EQ(pqp->LeftInput(), nullptr);

  const auto drop_table = std::dynamic_pointer_cast<DropTable>(pqp);
  EXPECT_EQ(drop_table->table_name, "t");
}

TEST_F(LqpTranslatorTest, CreatePreparedPlan) {
  const auto prepared_plan = std::make_shared<PreparedPlan>(DummyTableNode::Make(), std::vector<ParameterID>{});
  const auto lqp = CreatePreparedPlanNode::Make("p", prepared_plan);

  const auto pqp = LqpTranslator{}.translate_node(lqp);

  EXPECT_EQ(pqp->type(), OperatorType::CreatePreparedPlan);
  EXPECT_EQ(pqp->LeftInput(), nullptr);

  const auto prepare = std::dynamic_pointer_cast<CreatePreparedPlan>(pqp);
  EXPECT_EQ(prepare->prepared_plan(), prepared_plan);
}

TEST_F(LqpTranslatorTest, Export) {
  // clang-format off
  const auto lqp =
  ExportNode::Make("a_table", "a_file.tbl", FileType::Auto,
    ValidateNode::Make(int_float_node));
  // clang-format on

  const auto pqp = LqpTranslator{}.translate_node(lqp);
  const auto exporter = std::dynamic_pointer_cast<Export>(pqp);

  EXPECT_EQ(exporter->type(), OperatorType::Export);
  EXPECT_EQ(exporter->LeftInput()->type(), OperatorType::Validate);
}

TEST_F(LqpTranslatorTest, Import) {
  const auto lqp = ImportNode::Make("a_table", "a_file.tbl", FileType::Auto);

  const auto pqp = LqpTranslator{}.translate_node(lqp);
  const auto importer = std::dynamic_pointer_cast<Import>(pqp);

  EXPECT_EQ(importer->type(), OperatorType::Import);
  EXPECT_EQ(importer->LeftInput(), nullptr);
}

TEST_F(LqpTranslatorTest, ChangeMetaTable) {
  // clang-format off
  const auto lqp =
  ChangeMetaTableNode::Make("meta_table", MetaTableChangeType::Insert,
    DummyTableNode::Make(),
    DummyTableNode::Make());
  // clang-format on

  const auto pqp = LqpTranslator{}.translate_node(lqp);
  const auto change_meta_table = std::dynamic_pointer_cast<ChangeMetaTable>(pqp);

  EXPECT_EQ(change_meta_table->type(), OperatorType::ChangeMetaTable);
  EXPECT_EQ(change_meta_table->LeftInput()->type(), OperatorType::TableWrapper);
  EXPECT_EQ(change_meta_table->RightInput()->type(), OperatorType::TableWrapper);
}

}  // namespace skyrise
