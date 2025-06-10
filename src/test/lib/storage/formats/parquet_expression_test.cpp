#include "storage/formats/parquet_expression.hpp"

#include <gtest/gtest.h>

#include "expression/expression_functional.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

class ParquetExpressionTest : public ::testing::Test {};

TEST_F(ParquetExpressionTest, BinaryPredicates) {
  EXPECT_EQ(CreateArrowExpression(Equals_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(5))),
            arrow::compute::equal(arrow::compute::field_ref("a"), arrow::compute::literal(5)));
  EXPECT_EQ(CreateArrowExpression(NotEquals_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(5))),
            arrow::compute::not_equal(arrow::compute::field_ref("a"), arrow::compute::literal(5)));
  EXPECT_EQ(CreateArrowExpression(LessThan_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(5))),
            arrow::compute::less(arrow::compute::field_ref("a"), arrow::compute::literal(5)));
  EXPECT_EQ(CreateArrowExpression(LessThanEquals_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(5))),
            arrow::compute::less_equal(arrow::compute::field_ref("a"), arrow::compute::literal(5)));
  EXPECT_EQ(CreateArrowExpression(GreaterThan_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(5))),
            arrow::compute::greater(arrow::compute::field_ref("a"), arrow::compute::literal(5)));
  EXPECT_EQ(CreateArrowExpression(GreaterThanEquals_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(5))),
            arrow::compute::greater_equal(arrow::compute::field_ref("a"), arrow::compute::literal(5)));
}

TEST_F(ParquetExpressionTest, LogicalExpressions) {
  EXPECT_EQ(CreateArrowExpression(Or_(Equals_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(5)),
                                      Equals_(PqpColumn_(0, DataType::kInt, false, "b"), Value_(3)))),
            arrow::compute::or_(arrow::compute::equal(arrow::compute::field_ref("a"), arrow::compute::literal(5)),
                                arrow::compute::equal(arrow::compute::field_ref("b"), arrow::compute::literal(3))));
  EXPECT_EQ(CreateArrowExpression(And_(Equals_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(5)),
                                       Equals_(PqpColumn_(0, DataType::kInt, false, "b"), Value_(3)))),
            arrow::compute::and_(arrow::compute::equal(arrow::compute::field_ref("a"), arrow::compute::literal(5)),
                                 arrow::compute::equal(arrow::compute::field_ref("b"), arrow::compute::literal(3))));
}

TEST_F(ParquetExpressionTest, InBetweenExpressions) {
  EXPECT_EQ(
      CreateArrowExpression(BetweenInclusive_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(3), Value_(5))),
      arrow::compute::and_(arrow::compute::greater_equal(arrow::compute::field_ref("a"), arrow::compute::literal(3)),
                           arrow::compute::less_equal(arrow::compute::field_ref("a"), arrow::compute::literal(5))));
  EXPECT_EQ(
      CreateArrowExpression(BetweenLowerExclusive_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(3), Value_(5))),
      arrow::compute::and_(arrow::compute::greater(arrow::compute::field_ref("a"), arrow::compute::literal(3)),
                           arrow::compute::less_equal(arrow::compute::field_ref("a"), arrow::compute::literal(5))));
  EXPECT_EQ(
      CreateArrowExpression(BetweenUpperExclusive_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(3), Value_(5))),
      arrow::compute::and_(arrow::compute::greater_equal(arrow::compute::field_ref("a"), arrow::compute::literal(3)),
                           arrow::compute::less(arrow::compute::field_ref("a"), arrow::compute::literal(5))));
  EXPECT_EQ(CreateArrowExpression(BetweenExclusive_(PqpColumn_(0, DataType::kInt, false, "a"), Value_(3), Value_(5))),
            arrow::compute::and_(arrow::compute::greater(arrow::compute::field_ref("a"), arrow::compute::literal(3)),
                                 arrow::compute::less(arrow::compute::field_ref("a"), arrow::compute::literal(5))));
}

TEST_F(ParquetExpressionTest, IsNullExpressions) {
  EXPECT_EQ(CreateArrowExpression(IsNull_(PqpColumn_(0, DataType::kInt, false, "a"))),
            arrow::compute::is_null(arrow::compute::field_ref("a")));
  EXPECT_EQ(CreateArrowExpression(IsNotNull_(PqpColumn_(0, DataType::kInt, false, "a"))),
            arrow::compute::not_(arrow::compute::is_null(arrow::compute::field_ref("a"))));
}

TEST_F(ParquetExpressionTest, LiteralDatatypes) {
  EXPECT_EQ(CreateArrowExpression(Value_((int32_t)1337)), arrow::compute::literal(1337));
  EXPECT_EQ(CreateArrowExpression(Value_((int64_t)1337)), arrow::compute::literal((int64_t)1337));
  EXPECT_EQ(CreateArrowExpression(Value_((float)1337.2)), arrow::compute::literal((float)1337.2));
  EXPECT_EQ(CreateArrowExpression(Value_((double)1337.2)), arrow::compute::literal(1337.2));
  EXPECT_EQ(CreateArrowExpression(Value_("Hello World")), arrow::compute::literal("Hello World"));
}
}  // namespace skyrise
