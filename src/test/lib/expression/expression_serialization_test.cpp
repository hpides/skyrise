#include "expression/expression_serialization.hpp"

#include <gtest/gtest.h>

#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

namespace {

void TestSerializeAndDeserialize(const std::shared_ptr<AbstractExpression>& expression) {
  const std::string serialized_first_time = SerializeExpression(*expression).View().WriteCompact();
  EXPECT_FALSE(serialized_first_time.empty());

  const Aws::Utils::Json::JsonValue json_parsed(serialized_first_time);
  EXPECT_TRUE(json_parsed.WasParseSuccessful());

  const std::shared_ptr<AbstractExpression> deserialized_expression = DeserializeExpression(json_parsed.View());
  EXPECT_NE(deserialized_expression, nullptr);
  EXPECT_EQ(expression->GetExpressionType(), deserialized_expression->GetExpressionType());

  const std::string serialized_second_time = SerializeExpression(*deserialized_expression).View().WriteCompact();

  EXPECT_EQ(serialized_first_time, serialized_second_time);
  EXPECT_EQ(expression->Description(), deserialized_expression->Description());
}

void TestSerializeAndDeserialize(const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  for (const auto& expression : expressions) {
    TestSerializeAndDeserialize(expression);
  }
}

}  // namespace

/**
 * The following tests' expressions were taken from expression_evaluator_to_result_test.cpp
 */

TEST(ExpressionSerializationTest, AggregateExpressions) {
  const auto column_a = std::make_shared<PqpColumnExpression>(1, DataType::kInt, false, "a");
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      std::make_shared<AggregateExpression>(AggregateFunction::kAvg, ToExpression(5)),
      std::make_shared<AggregateExpression>(AggregateFunction::kAvg, column_a),
      std::make_shared<AggregateExpression>(
          AggregateFunction::kCount,
          std::make_shared<PqpColumnExpression>(kInvalidColumnId, DataType::kNull, false, "*"))};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, ArithmeticExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {Mul_(5, Add_(2, 2.5)), Div_(Sub_(10, 2), 2)};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, BetweenExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      BetweenInclusive_(5.0f, 3.1, 5), BetweenLowerExclusive_(4, 3.0, 5.0), BetweenLowerExclusive_(3, 3, Null_())};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, CaseExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      Case_(ToExpression(1), ToExpression(2), ToExpression(3))};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, CastExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {Cast_(ToExpression(6.5f), DataType::kInt),
                                                                        Cast_(ToExpression(6), DataType::kFloat)};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, ExpressionPointer) {
  const std::shared_ptr<AbstractExpression> valid_pointer = ToExpression(10);
  const std::shared_ptr<AbstractExpression> null_pointer = nullptr;

  const Aws::Utils::Json::JsonValue serialized_valid_pointer = SerializeExpression(valid_pointer);
  EXPECT_TRUE(serialized_valid_pointer.View().ValueExists("type"));
  EXPECT_ANY_THROW(SerializeExpression(null_pointer));
}

TEST(ExpressionSerializationTest, ExtractExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      Extract_(DatetimeComponent::kMonth, "1992-09-30"), Extract_(DatetimeComponent::kYear, Null_()),
      Extract_(DatetimeComponent::kHour, "1992-09-30"), Extract_(DatetimeComponent::kSecond, "1992-09-30")};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, InAndListExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      List_(Null_(), 2, 3, 4), In_(Null_(), List_(Null_())), In_("You", List_("Hello", 1.0, "You", 3.0)),
      In_(5, List_(1.0, Add_(2.0, 3.0))), NotIn_(Null_(), List_(Null_()))};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, IsNullExpressionExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {IsNull_(0), IsNull_(Null_()), IsNotNull_(1)};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, LiteralExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {ToExpression(10),    ToExpression(10.0f),
                                                                        ToExpression(10.0f), ToExpression("10"),
                                                                        ToExpression(10L),   ToExpression(kNullValue)};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, LogicalExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {And_(1, 1), And_(NullValue(), NullValue()),
                                                                        Or_(1, 1)};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, PredicateExpressions) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {GreaterThan_(5, 2), Equals_(10, 10)};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, PqpColumnExpressions) {
  const auto column_a = std::make_shared<PqpColumnExpression>(1, DataType::kInt, false, "a");
  const auto column_b = std::make_shared<PqpColumnExpression>(2, DataType::kDouble, false, "b");
  const auto column_c = std::make_shared<PqpColumnExpression>(2, DataType::kString, false, "c");

  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      column_a, column_b, column_c, GreaterThan_(column_b, column_a),
      In_(Sub_(Mul_(column_a, 2), 2), List_(column_b, 6, Null_(), 0))};
  TestSerializeAndDeserialize(expressions);
}

TEST(ExpressionSerializationTest, UnaryMinusExpressions) {
  const auto column_a = std::make_shared<PqpColumnExpression>(1, DataType::kInt, false, "a");
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {UnaryMinus_(2.5), UnaryMinus_(int32_t{-3}),
                                                                        UnaryMinus_(column_a)};
  TestSerializeAndDeserialize(expressions);
}

}  // namespace skyrise
