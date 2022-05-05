#include "compiler/logical_query_plan/lqp_expression_utils.hpp"

#include <gtest/gtest.h>

#include "compiler/logical_query_plan/lqp_expression_utils.hpp"
#include "compiler/logical_query_plan/mock_node.hpp"
#include "compiler/logical_query_plan/stored_table_node.hpp"
#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "metadata/mock_catalog.hpp"

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

namespace skyrise {

class LqpExpressionUtilsTest : public ::testing::Test {
 public:
  void SetUp() override {
    mock_catalog_ = std::make_shared<MockCatalog>();
    mock_catalog_->AddTableSchemaFromFileHeader("int_float", "resources/test_data/tbl/int_float.tbl");

    int_float_node_ = StoredTableNode::Make("int_float", mock_catalog_);
    a_ = int_float_node_->GetColumn("a");
    b_ = int_float_node_->GetColumn("b");
  }

 protected:
  std::shared_ptr<MockCatalog> mock_catalog_;
  std::shared_ptr<LqpColumnExpression> a_, b_;
  std::shared_ptr<StoredTableNode> int_float_node_;
};

TEST_F(LqpExpressionUtilsTest, IsCountStarAggregateExpression) {
  EXPECT_TRUE(IsCountStarAggregateExpression(CountStarLqp_(int_float_node_)));
  EXPECT_FALSE(IsCountStarAggregateExpression(Count_(a_)));
  EXPECT_FALSE(IsCountStarAggregateExpression(Sum_(a_)));
  EXPECT_FALSE(IsCountStarAggregateExpression(Add_(a_, 1)));
  EXPECT_FALSE(IsCountStarAggregateExpression(a_));
}

TEST_F(LqpExpressionUtilsTest, ExpressionIsNullableOnLqp) {
  const auto lqp = MockNode::Make(MockNode::ColumnDefinitions{});
  EXPECT_FALSE(ExpressionIsNullableOnLqp(Add_(1, 2), *lqp));
  EXPECT_FALSE(ExpressionIsNullableOnLqp(BetweenInclusive_(1, 2, 3), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(BetweenInclusive_(1, Null_(), 3), *lqp));
  EXPECT_FALSE(ExpressionIsNullableOnLqp(List_(1, 2), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(List_(1, Null_()), *lqp));
  EXPECT_FALSE(ExpressionIsNullableOnLqp(And_(1, 1), *lqp));
  //  EXPECT_FALSE(ExpressionIsNullableOnLqp(case_(1, 1, 2), *lqp));
  //  EXPECT_TRUE(ExpressionIsNullableOnLqp(case_(Null_(), 1, 2), *lqp));
  //  EXPECT_TRUE(ExpressionIsNullableOnLqp(case_(1, 1, Null_()), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(Add_(GreaterThan_(2, Null_()), 1), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(And_(GreaterThan_(2, Null_()), 1), *lqp));
  EXPECT_FALSE(ExpressionIsNullableOnLqp(Cast_(12, DataType::kString), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(Cast_(Null_(), DataType::kString), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(Sum_(Null_()), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(Sum_(Add_(1, 2)), *lqp));
  EXPECT_FALSE(ExpressionIsNullableOnLqp(CountStarLqp_(lqp), *lqp));
  EXPECT_FALSE(ExpressionIsNullableOnLqp(Count_(5), *lqp));
  EXPECT_FALSE(ExpressionIsNullableOnLqp(Count_(Null_()), *lqp));
  EXPECT_FALSE(ExpressionIsNullableOnLqp(In_(1, List_(1, 2, 3)), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(In_(Null_(), List_(1, 2, 3)), *lqp));

  // Division by zero could be nullable, thus division and modulo are always nullable
  EXPECT_TRUE(ExpressionIsNullableOnLqp(Div_(1, 2), *lqp));
  EXPECT_TRUE(ExpressionIsNullableOnLqp(Mod_(1, 2), *lqp));
}

}  // namespace skyrise
