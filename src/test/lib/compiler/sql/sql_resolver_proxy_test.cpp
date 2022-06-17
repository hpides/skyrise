#include <memory>

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/sql/parameter_id_allocator.hpp"
#include "compiler/sql/sql_identifier_resolver.hpp"
#include "compiler/sql/sql_identifier_resolver_proxy.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"

using namespace std::string_literals;            // NOLINT(google-build-using-namespace)
using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class SqlIdentifierResolverTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::Make(MockNode::ColumnDefinitions{
        {{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}, {DataType::kInt, "d"}}});
    node_b = MockNode::Make(
        MockNode::ColumnDefinitions{{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}}});
    node_c = MockNode::Make(
        MockNode::ColumnDefinitions{{{DataType::kInt, "a"}, {DataType::kInt, "b"}, {DataType::kInt, "c"}}});

    expression_a = std::make_shared<LqpColumnExpression>(node_a, ColumnId{0});
    expression_b = std::make_shared<LqpColumnExpression>(node_a, ColumnId{1});
    expression_c = std::make_shared<LqpColumnExpression>(node_a, ColumnId{2});
    expression_unnamed = std::make_shared<LqpColumnExpression>(node_a, ColumnId{3});

    context.AddColumnName(expression_a, {"a"s});
    context.AddColumnName(expression_b, {"b"s});
    context.AddColumnName(expression_c, {"c"s});
    context.SetTableName(expression_a, {"T1"s});
    context.SetTableName(expression_b, {"T1"s});
    context.SetTableName(expression_c, {"T2"s});

    parameter_id_allocator = std::make_shared<ParameterIDAllocator>();
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  std::shared_ptr<AbstractExpression> expression_a, expression_b, expression_c, expression_unnamed;
  SqlIdentifierResolver context;
  std::shared_ptr<ParameterIDAllocator> parameter_id_allocator;
};

TEST_F(SqlIdentifierResolverTest, ResolveIdentifier) {
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s}), expression_a);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"b"s}), expression_b);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"c"s}), expression_c);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "T1"}), expression_a);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "T2"}), nullptr);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"x"s}), nullptr);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"x"s, "T1"}), nullptr);
}

TEST_F(SqlIdentifierResolverTest, ColumnNamesChange) {
  context.AddColumnName(expression_a, "x");

  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s}), expression_a);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "T1"}), expression_a);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"x"s, "T1"}), expression_a);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"x"s, "T2"}), nullptr);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"x"s}), expression_a);

  EXPECT_EQ(context.ResolveIdentifierRelaxed({"b"s}), expression_b);
}

TEST_F(SqlIdentifierResolverTest, ResetColumnNames) {
  context.ResetColumnNames(expression_a);

  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s}), nullptr);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "T1"}), nullptr);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"b"s}), expression_b);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"b"s, "T1"}), expression_b);
}

TEST_F(SqlIdentifierResolverTest, TableNameChanges) {
  context.AddColumnName(expression_a, "x");
  context.SetTableName(expression_a, "X");

  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s}), expression_a);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "T1"}), nullptr);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "X"}), expression_a);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"x"s}), expression_a);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"x"s, "T1"}), nullptr);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"x"s, "X"}), expression_a);

  EXPECT_EQ(context.ResolveIdentifierRelaxed({"b"s}), expression_b);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"b"s, "T1"}), expression_b);
}

TEST_F(SqlIdentifierResolverTest, ColumnNameRedundancy) {
  auto expression_a2 = std::make_shared<LqpColumnExpression>(node_c, ColumnId{2});

  context.AddColumnName(expression_a2, {"a"s});

  // "a" is ambiguous now
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s}), nullptr);

  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "T1"}), expression_a);

  context.SetTableName(expression_a2, "T2");
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "T2"}), expression_a2);
}

/**
 * TODO(anyone) Enable when correlated subqueries are implemented.
 */
// TEST_F(SqlIdentifierResolverTest, ResolveOuterExpression) {
//  /**
//   * Simulate a scenario in which a Sub-Subquery accesses an Identifier from the outermost and intermediate queries
//   */
//
//  /**
//   * Create context and context proxy for the outermost query
//   */
//  const auto outermost_expression_a = std::make_shared<LqpColumnExpression>(node_b, ColumnId{0});
//  const auto outermost_expression_b = std::make_shared<LqpColumnExpression>(node_b, ColumnId{1});
//  const auto outermost_expression_c = std::make_shared<LqpColumnExpression>(node_b, ColumnId{2});
//  const auto outermost_context = std::make_shared<SqlIdentifierResolver>();
//  outermost_context->AddColumnName(outermost_expression_a, "outermost_a");
//  outermost_context->AddColumnName(outermost_expression_b, "b");  // Intentionally named just "b"
//  outermost_context->AddColumnName(outermost_expression_c, "c");  // Intentionally named just "c"
//  outermost_context->SetTableName(outermost_expression_b, "Outermost");
//
//  const auto outermost_context_proxy =
//      std::make_shared<SqlIdentifierResolverProxy>(outermost_context, parameter_id_allocator);
//
//  /**
//   * Create context and context proxy for the nested ("intermediate") query
//   */
//  auto intermediate_context = std::make_shared<SqlIdentifierResolver>();
//  const auto intermediate_expression_a = std::make_shared<LqpColumnExpression>(node_c, ColumnId{0});
//  const auto intermediate_expression_b = std::make_shared<LqpColumnExpression>(node_c, ColumnId{1});
//  intermediate_context->AddColumnName(intermediate_expression_a, "intermediate_a");
//  intermediate_context->AddColumnName(intermediate_expression_b, "b");  // Intentionally named just "b"
//  intermediate_context->SetTableName(intermediate_expression_b, "Intermediate");
//
//  const auto intermediate_context_proxy = std::make_shared<SqlIdentifierResolverProxy>(
//      intermediate_context, parameter_id_allocator, outermost_context_proxy);
//
//  /**
//   * Test whether identifiers are resolved correctly
//   */
//  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"}), expression_a);
//  EXPECT_EQ(context.ResolveIdentifierRelaxed({"b"}), expression_b);
//  EXPECT_EQ(context.ResolveIdentifierRelaxed({"b", "T1"}), expression_b);
//
//  EXPECT_EQ(*intermediate_context_proxy->ResolveIdentifierRelaxed({"b", "Intermediate"}),
//            *correlated_parameter_(ParameterID{0}, intermediate_expression_b));
//  EXPECT_EQ(*intermediate_context_proxy->ResolveIdentifierRelaxed({"intermediate_a"}),
//            *correlated_parameter_(ParameterID{1}, intermediate_expression_a));
//  EXPECT_EQ(*intermediate_context_proxy->ResolveIdentifierRelaxed({"b"}),
//            *correlated_parameter_(ParameterID{0}, intermediate_expression_b));
//  EXPECT_EQ(intermediate_context_proxy->ResolveIdentifierRelaxed({"intermediate_a", "Intermediate"}), nullptr);
//
//  EXPECT_EQ(*intermediate_context_proxy->ResolveIdentifierRelaxed({"outermost_a"}),
//            *correlated_parameter_(ParameterID{2}, outermost_expression_a));
//  EXPECT_EQ(*intermediate_context_proxy->ResolveIdentifierRelaxed({"b", "Outermost"}),
//            *correlated_parameter_(ParameterID{3}, outermost_expression_b));
//
//  /**
//   * Test whether the proxies tracked accesses to their contexts correctly
//   */
//  ASSERT_EQ(outermost_context_proxy->accessed_expressions().size(), 2u);
//  EXPECT_EQ(outermost_context_proxy->accessed_expressions().count(outermost_expression_a), 1u);
//  EXPECT_EQ(outermost_context_proxy->accessed_expressions().count(outermost_expression_b), 1u);
//
//  ASSERT_EQ(intermediate_context_proxy->accessed_expressions().size(), 2u);
//  EXPECT_EQ(intermediate_context_proxy->accessed_expressions().count(intermediate_expression_b), 1u);
//  EXPECT_EQ(intermediate_context_proxy->accessed_expressions().count(intermediate_expression_a), 1u);
//}

TEST_F(SqlIdentifierResolverTest, GetExpressionIdentifiers) {
  EXPECT_EQ(context.GetExpressionIdentifiers(expression_a), std::vector<SqlIdentifier>{SqlIdentifier("a", "T1")});
  EXPECT_EQ(context.GetExpressionIdentifiers(expression_unnamed), std::vector<SqlIdentifier>{});
}

TEST_F(SqlIdentifierResolverTest, DeepEqualsIsUsed) {
  /**
   * Test that we can use equivalent Expression objects that are stored in different Objects
   */

  const auto expression_a2 = std::make_shared<LqpColumnExpression>(node_a, ColumnId{0});
  const std::vector<SqlIdentifier> expressions = {SqlIdentifier("a"s, "T2"), SqlIdentifier("a2"s, "T2")};
  context.AddColumnName(expression_a2, "a2");
  context.SetTableName(expression_a2, "T2");
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a"s, "T2"}), expression_a);
  EXPECT_EQ(context.GetExpressionIdentifiers(expression_a), expressions);
  EXPECT_EQ(context.ResolveIdentifierRelaxed({"a2"s, "T2"}), expression_a);
  EXPECT_EQ(context.GetExpressionIdentifiers(expression_a2), expressions);
}

TEST_F(SqlIdentifierResolverTest, ResolveTableName) {
  /**
   * Test that all Expressions of a table name can be found
   */

  const auto t1_expressions = std::vector<std::shared_ptr<AbstractExpression>>({expression_a, expression_b});
  const auto t2_expressions = std::vector<std::shared_ptr<AbstractExpression>>({expression_c});
  EXPECT_EQ(context.ResolveTableName("T1"), t1_expressions);
  EXPECT_EQ(context.ResolveTableName("T2"), t2_expressions);
}

}  // namespace skyrise
