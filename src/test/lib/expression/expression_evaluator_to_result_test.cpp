/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include <gtest/gtest.h>

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "storage/backend/test_storage.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"
#include "testing/load_table.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

class ExpressionEvaluatorToResultTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = std::make_shared<TestStorage>();
    // Load table_a_.
    table_a_ = LoadTable<CsvFormatReader>("csv/input_a.csv", storage_);
    a_ = PqpColumnExpression::FromTable(*table_a_, std::string("a"));
    b_ = PqpColumnExpression::FromTable(*table_a_, std::string("b"));
    d_ = PqpColumnExpression::FromTable(*table_a_, std::string("d"));
    e_ = PqpColumnExpression::FromTable(*table_a_, std::string("e"));
    f_ = PqpColumnExpression::FromTable(*table_a_, std::string("f"));
    s1_ = PqpColumnExpression::FromTable(*table_a_, std::string("s1"));
    s2_ = PqpColumnExpression::FromTable(*table_a_, std::string("s2"));
    dates_ = PqpColumnExpression::FromTable(*table_a_, std::string("dates"));

    // Load table_b_.
    table_b_ = LoadTable<CsvFormatReader>("csv/expression_evaluator/input_b.csv", storage_);
    g_ = PqpColumnExpression::FromTable(*table_b_, "g");

    // Load table_bools_.
    table_bools_ = LoadTable<CsvFormatReader>("csv/expression_evaluator/input_bools.csv", storage_);
    bool_a_ = PqpColumnExpression::FromTable(*table_bools_, "a");
    bool_b_ = PqpColumnExpression::FromTable(*table_bools_, "b");

    // Create table_empty_.
    TableColumnDefinitions empty_table_columns;
    empty_table_columns.emplace_back("a", DataType::kInt, false);
    empty_table_columns.emplace_back("b", DataType::kFloat, true);
    empty_table_columns.emplace_back("s", DataType::kString, false);
    table_empty_ = std::make_shared<Table>(empty_table_columns);

    Segments segments;
    segments.push_back(std::make_shared<ValueSegment<int32_t>>(std::vector<int32_t>()));
    segments.push_back(std::make_shared<ValueSegment<float>>(std::vector<float>(), std::vector<bool>()));
    segments.push_back(std::make_shared<ValueSegment<std::string>>(std::vector<std::string>()));
    table_empty_->AppendChunk(segments);

    empty_a_ = PqpColumnExpression::FromTable(*table_empty_, "a");
    empty_b_ = PqpColumnExpression::FromTable(*table_empty_, "b");
    empty_s_ = PqpColumnExpression::FromTable(*table_empty_, "s");

    // Create expressions.
    a_plus_b_ = std::make_shared<ArithmeticExpression>(ArithmeticOperator::kAddition, a_, b_);
    a_lt_b_ = std::make_shared<BinaryPredicateExpression>(PredicateCondition::kLessThan, a_, b_);
    s1_gt_s2_ = std::make_shared<BinaryPredicateExpression>(PredicateCondition::kGreaterThan, s1_, s2_);
    s1_lt_s2_ = std::make_shared<BinaryPredicateExpression>(PredicateCondition::kLessThan, s1_, s2_);
  }

  /**
   * Turn an ExpressionResult<T> into std::vector<std::optional<T>> to make the writing of tests easier.
   */
  template <typename T>
  std::vector<std::optional<T>> NormalizeExpressionResult(const ExpressionResult<T>& result) {
    std::vector<std::optional<T>> normalized(result.Size());

    result.AsView([&](const auto& resolved) {
      for (size_t i = 0; i < result.Size(); ++i) {
        if (!resolved.IsNull(i)) {
          normalized[i] = resolved.Value(i);
        }
      }
    });

    return normalized;
  }

  template <typename R>
  void Print(const std::vector<std::optional<R>>& values_or_nulls) {
    for (const auto& value_or_null : values_or_nulls) {
      if (value_or_null) {
        std::cout << *value_or_null << ", ";
      } else {
        std::cout << "NULL, ";
      }
    }
    std::cout << "\n";
  }

  template <typename R>
  bool TestExpression(const std::shared_ptr<Table>& table, const AbstractExpression& expression,
                      const std::vector<std::optional<R>>& expected) {
    const auto actual_result = ExpressionEvaluator(table, ChunkId{0}).EvaluateExpressionToResult<R>(expression);
    const auto actual_normalized = NormalizeExpressionResult(*actual_result);
    if (actual_normalized == expected) {
      return true;
    }

    std::cout << "Actual:\n";
    Print(actual_normalized);
    std::cout << "Expected:\n";
    Print(expected);

    return false;
  }

  template <typename R>
  bool TestExpression(const AbstractExpression& expression, const std::vector<std::optional<R>>& expected) {
    const auto actual_result = ExpressionEvaluator().EvaluateExpressionToResult<R>(expression);
    const auto actual_normalized = NormalizeExpressionResult(*actual_result);
    if (actual_normalized == expected) {
      return true;
    }

    std::cout << "Actual:\n";
    Print(actual_normalized);
    std::cout << "Expected:\n";
    Print(expected);

    return false;
  }

  std::shared_ptr<Table> table_a_, table_b_, table_bools_, table_empty_;
  std::shared_ptr<PqpColumnExpression> a_, b_, d_, e_, f_, s1_, s2_, dates_, g_, bool_a_, bool_b_;
  std::shared_ptr<PqpColumnExpression> empty_a_, empty_b_, empty_s_;
  std::shared_ptr<ArithmeticExpression> a_plus_b_;
  std::shared_ptr<BinaryPredicateExpression> a_lt_b_;
  std::shared_ptr<BinaryPredicateExpression> s1_gt_s2_;
  std::shared_ptr<BinaryPredicateExpression> s1_lt_s2_;
  std::shared_ptr<TestStorage> storage_;
};

TEST_F(ExpressionEvaluatorToResultTest, TernaryOrLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(1, 0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(1, 1), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(0, 1), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(0, 0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(0, NullValue()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(1, NullValue()), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(NullValue(), NullValue()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(NullValue(), 0), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Or_(NullValue(), 1), {1}));
}

TEST_F(ExpressionEvaluatorToResultTest, TernaryOrSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_bools_, *Or_(bool_a_, bool_b_), {0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
}

TEST_F(ExpressionEvaluatorToResultTest, TernaryAndLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*And_(1, 0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*And_(1, 1), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*And_(0, 1), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*And_(0, 0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*And_(0, NullValue()), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*And_(1, NullValue()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*And_(NullValue(), NullValue()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*And_(NullValue(), 0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*And_(NullValue(), 1), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorToResultTest, TernaryAndSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_bools_, *And_(bool_a_, bool_b_), {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1}));
}

TEST_F(ExpressionEvaluatorToResultTest, ValueLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*Value_(5), {5}));
  EXPECT_TRUE(TestExpression<float>(*Value_(5.0f), {5.0f}));
  EXPECT_TRUE(TestExpression<int32_t>(*Value_(NullValue()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<float>(*Value_(NullValue()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<std::string>(*Value_("Hello"), {"Hello"}));
  EXPECT_TRUE(TestExpression<std::string>(*Value_(NullValue()), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorToResultTest, ArithmeticsLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*Mul_(5, 3), {15}));
  EXPECT_TRUE(TestExpression<int32_t>(*Mul_(5, NullValue()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Add_(5, 6), {11}));
  EXPECT_TRUE(TestExpression<float>(*Add_(5, 6), {11.0}));
  EXPECT_TRUE(TestExpression<int32_t>(*Sub_(15, 12), {3}));
  EXPECT_TRUE(TestExpression<float>(*Div_(10.0, 4.0), {2.5f}));
  EXPECT_TRUE(TestExpression<float>(*Div_(10.0, 0), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Div_(10, 0), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Sub_(NullValue(), NullValue()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Mod_(5, 3), {2}));
  EXPECT_TRUE(TestExpression<float>(*Mod_(23.25, 3), {2.25}));
  EXPECT_TRUE(TestExpression<float>(*Mod_(23.25, 0), {std::nullopt}));
  EXPECT_TRUE(TestExpression<float>(*Mod_(5, 0), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorToResultTest, ArithmeticsSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *Mul_(a_, b_), {2, 6, 12, 20}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *Mod_(b_, a_), {0, 1, 1, 1}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *Add_(a_, NullValue()),
                                      {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *Add_(a_, Add_(b_, NullValue())),
                                      {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
}

TEST_F(ExpressionEvaluatorToResultTest, ExpressionReuse) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *Add_(Mul_(a_, b_), Mul_(a_, b_)), {4, 12, 24, 40}));
}

TEST_F(ExpressionEvaluatorToResultTest, PredicatesLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_(5, 3.3), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_(5, 5.0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_(5.1, 5.0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_(Null_(), 5.0), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_(5.0, Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_(Null_(), Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_("Hello", "Wello"), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_("Wello", "Hello"), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThan_("Wello", Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThanEquals_(5.3, 3), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThanEquals_(5.3, 5.3), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThanEquals_(5.3, 5.4f), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*GreaterThanEquals_(5.5f, 5.4), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*LessThan_(5.2f, 5.4), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*LessThan_(5.5f, 5.4), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*LessThan_(5.4, 5.4), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*LessThanEquals_(5.3, 5.4), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*LessThanEquals_(5.4, 5.4), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*LessThanEquals_(5.5, 5.4), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*Equals_(5.5f, 5.5f), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*Equals_(5.5f, 5.7f), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*Equals_("Hello", "Hello"), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*Equals_("Hello", "hello"), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*Equals_("Hello", Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotEquals_(5.5f, 5), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotEquals_(5.5f, 5.5f), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(4, 3.0, 5.0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(3, 3.0, 5.0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(3, 3.1, 5.0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(5.0f, 3.1, 5), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(5.1f, 3.1, 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(5.1f, 3.1, Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(5.1f, Null_(), 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(Null_(), 3.1, 5), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenInclusive_(Null_(), Null_(), Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(4, 3.0, 5.0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(3, 3.0, 5.0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(3, 3.1, 5.0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(5.0f, 3.1, 5), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(5.1f, 3.1, 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(3.1f, 3, Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(5, Null_(), 5.1f), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(3, 3, Null_()), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(5.1f, Null_(), 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(Null_(), 3.1, 5), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenLowerExclusive_(Null_(), Null_(), Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(4, 3.0, 5.0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(3, 3.0, 5.0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(3, 3.1, 5.0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(5, 3.1, 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(5, 3.1, 5.1), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(3.1, 3, Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(3, 3.1, Null_()), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(5, Null_(), 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(5, Null_(), 5.1f), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(Null_(), 3.1, 5), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenUpperExclusive_(Null_(), Null_(), Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(4, 3.0, 5.0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(3, 3, 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(5, 3, 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(5, 3, Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(3, 3, Null_()), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(5, Null_(), 5), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(4, Null_(), 5), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(Null_(), 3, 5), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*BetweenExclusive_(Null_(), Null_(), Null_()), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorToResultTest, PredicatesSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *GreaterThan_(b_, a_), {1, 1, 1, 1}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *GreaterThan_(s1_, s2_), {0, 0, 1, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *GreaterThan_(b_, Null_()),
                                      {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *GreaterThanEquals_(b_, Mul_(a_, 2)), {1, 0, 0, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *Equals_(b_, Mul_(a_, 2)), {1, 0, 0, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *NotEquals_(b_, Mul_(a_, 2)), {0, 1, 1, 1}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *LessThan_(b_, Mul_(a_, 2)), {0, 1, 1, 1}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *LessThanEquals_(b_, Mul_(a_, 2)), {1, 1, 1, 1}));

  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenInclusive_(e_, a_, f_), {1, 0, 0, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenInclusive_(3.3, a_, b_), {0, 0, 1, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenInclusive_(4, a_, b_), {0, 0, 1, 1}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenInclusive_(b_, a_, d_), {1, 1, 1, 1}));

  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenLowerExclusive_(a_, a_, b_), {0, 0, 0, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenLowerExclusive_(a_, Sub_(a_, 1), b_), {1, 1, 1, 1}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenLowerExclusive_(2, a_, b_), {1, 0, 0, 0}));

  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenUpperExclusive_(a_, a_, b_), {1, 1, 1, 1}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenUpperExclusive_(a_, a_, Sub_(b_, 1)), {0, 0, 0, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenUpperExclusive_(2, a_, b_), {0, 1, 0, 0}));

  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenExclusive_(2, a_, b_), {0, 0, 0, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenExclusive_(2.5, a_, b_), {0, 1, 0, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenExclusive_(a_, a_, b_), {0, 0, 0, 0}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *BetweenExclusive_(Add_(a_, 0.5f), a_, b_), {1, 1, 1, 1}));
}

TEST_F(ExpressionEvaluatorToResultTest, IsNullLiteral) {
  EXPECT_TRUE(TestExpression<int32_t>(*IsNull_(0), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*IsNull_(1), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*IsNull_(Null_()), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*IsNotNull_(0), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*IsNotNull_(1), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*IsNotNull_(Null_()), {0}));
}

TEST_F(ExpressionEvaluatorToResultTest, NegateLiteral) {
  EXPECT_TRUE(TestExpression<double>(*UnaryMinus_(2.5), {-2.5}));
  EXPECT_TRUE(TestExpression<int32_t>(*UnaryMinus_(int32_t{-3}), {int32_t{3}}));
}

TEST_F(ExpressionEvaluatorToResultTest, NegateSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *UnaryMinus_(a_), {-1, -2, -3, -4}));
}

TEST_F(ExpressionEvaluatorToResultTest, InListLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*In_(Null_(), List_(Null_())), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(Null_(), List_(Null_(), 3)), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(Null_(), List_()), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(Null_(), List_(1, 2, 3, 4)), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(Null_(), List_(Null_(), 2, 3, 4)), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_(Null_(), 5, Null_())), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_()), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_(Null_(), Add_(2, 3), Null_())), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_(Null_(), 6, Null_())), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_(1, 3)), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_(1.0, 3.0)), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_("Hello", 1.0, "You", 3.0)), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_("You", List_("Hello", 1.0, "You", 3.0)), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_(1.0, 5.0)), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_(1.0, Add_(1.0, 3.0))), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*In_(5, List_(1.0, Add_(2.0, 3.0))), {1}));
}

TEST_F(ExpressionEvaluatorToResultTest, InListSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *In_(a_, List_(1.0, 3.0)), {1, 0, 1, 0}));
  EXPECT_TRUE(
      TestExpression<int32_t>(table_a_, *In_(a_, List_(Null_(), 1.0, 3.0)), {1, std::nullopt, 1, std::nullopt}));
  EXPECT_TRUE(
      TestExpression<int32_t>(table_a_, *In_(Sub_(Mul_(a_, 2), 2), List_(b_, 6, Null_(), 0)), {1, std::nullopt, 1, 1}));
}

TEST_F(ExpressionEvaluatorToResultTest, InArbitraryExpression) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *In_(a_, Div_(b_, 2.0f)), {1, 0, 0, 0}));
}

TEST_F(ExpressionEvaluatorToResultTest, NotInListLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(Null_(), List_(Null_())), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(Null_(), List_(Null_(), 3)), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(Null_(), List_()), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(Null_(), List_(1, 2, 3, 4)), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(Null_(), List_(Null_(), 2, 3, 4)), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_(Null_(), 5, Null_())), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_()), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_(Null_(), Add_(2, 3), Null_())), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_(Null_(), 6, Null_())), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_(1, 3)), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_(1.0, 3.0)), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_("Hello", 1.0, "You", 3.0)), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_("You", List_("Hello", 1.0, "You", 3.0)), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_(1.0, 5.0)), {0}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_(1.0, Add_(1.0, 3.0))), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*NotIn_(5, List_(1.0, Add_(2.0, 3.0))), {0}));
}

TEST_F(ExpressionEvaluatorToResultTest, NotInListSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *NotIn_(a_, List_(1.0, 3.0)), {0, 1, 0, 1}));
  EXPECT_TRUE(
      TestExpression<int32_t>(table_a_, *NotIn_(a_, List_(Null_(), 1.0, 3.0)), {0, std::nullopt, 0, std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *NotIn_(Sub_(Mul_(a_, 2), 2), List_(b_, 6, Null_(), 0)),
                                      {0, std::nullopt, 0, 0}));
}

TEST_F(ExpressionEvaluatorToResultTest, NotInArbitraryExpression) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *NotIn_(a_, Div_(b_, 2.0f)), {0, 1, 1, 1}));
}

TEST_F(ExpressionEvaluatorToResultTest, ExtractLiterals) {
  EXPECT_TRUE(TestExpression<std::string>(*Extract_(DatetimeComponent::kYear, "1992-09-30"), {"1992"}));
  EXPECT_TRUE(TestExpression<std::string>(*Extract_(DatetimeComponent::kMonth, "1992-09-30"), {"09"}));
  EXPECT_TRUE(TestExpression<std::string>(*Extract_(DatetimeComponent::kDay, "1992-09-30"), {"30"}));
  EXPECT_TRUE(TestExpression<std::string>(*Extract_(DatetimeComponent::kYear, Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<std::string>(*Extract_(DatetimeComponent::kMonth, Null_()), {std::nullopt}));
  EXPECT_TRUE(TestExpression<std::string>(*Extract_(DatetimeComponent::kDay, Null_()), {std::nullopt}));

  EXPECT_THROW(TestExpression<std::string>(*Extract_(DatetimeComponent::kHour, "1992-09-30"), {"30"}),
               std::logic_error);
  EXPECT_THROW(TestExpression<std::string>(*Extract_(DatetimeComponent::kMinute, "1992-09-30"), {"30"}),
               std::logic_error);
  EXPECT_THROW(TestExpression<std::string>(*Extract_(DatetimeComponent::kSecond, "1992-09-30"), {"30"}),
               std::logic_error);

  EXPECT_EQ(Extract_(DatetimeComponent::kYear, "1993-08-01")->GetDataType(), DataType::kString);
}

TEST_F(ExpressionEvaluatorToResultTest, ExtractSeries) {
  EXPECT_TRUE(TestExpression<std::string>(table_a_, *Extract_(DatetimeComponent::kYear, dates_),
                                          {"2017", "2014", "2011", "2010"}));
  EXPECT_TRUE(
      TestExpression<std::string>(table_a_, *Extract_(DatetimeComponent::kMonth, dates_), {"12", "08", "09", "01"}));
  EXPECT_TRUE(
      TestExpression<std::string>(table_a_, *Extract_(DatetimeComponent::kDay, dates_), {"06", "05", "03", "02"}));
}

TEST_F(ExpressionEvaluatorToResultTest, CastLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*Cast_(5.5, DataType::kInt), {5}));
  EXPECT_TRUE(TestExpression<float>(*Cast_(5.5, DataType::kFloat), {5.5f}));
  EXPECT_TRUE(TestExpression<float>(*Cast_(5, DataType::kFloat), {5.0f}));
  EXPECT_TRUE(TestExpression<std::string>(*Cast_(5.5, DataType::kString), {"5.5"}));
  EXPECT_TRUE(TestExpression<int32_t>(*Cast_(Null_(), DataType::kInt), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Cast_("Hello", DataType::kInt), {0}));
  EXPECT_TRUE(TestExpression<float>(*Cast_("Hello", DataType::kFloat), {0.0f}));
}

TEST_F(ExpressionEvaluatorToResultTest, CastSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *Cast_(a_, DataType::kInt), {1, 2, 3, 4}));
  EXPECT_TRUE(TestExpression<float>(table_a_, *Cast_(a_, DataType::kFloat), {1.0f, 2.0f, 3.0f, 4.0f}));
  EXPECT_TRUE(TestExpression<std::string>(table_a_, *Cast_(a_, DataType::kString), {"1", "2", "3", "4"}));
  EXPECT_TRUE(TestExpression<int32_t>(table_a_, *Cast_(f_, DataType::kInt), {99, 2, 13, 15}));
}

TEST_F(ExpressionEvaluatorToResultTest, CaseLiterals) {
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(1, 2, 1), {2}));
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(1, NullValue{}, 1), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(1, 2.3, 1), {2.3}));
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(0, 2.3, 1), {1.0}));
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(0, 2.3, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(0, 2, 1), {1}));
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(0, 2, Case_(1, 5, 13)), {5}));
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(NullValue{}, 42, Add_(5, 3)), {8}));
  EXPECT_TRUE(TestExpression<int32_t>(*Case_(1, NullValue{}, 5), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorToResultTest, CaseSeries) {
  EXPECT_TRUE(TestExpression<int32_t>(table_empty_, *Case_(GreaterThan_(empty_a_, 3), 1, 2), {}));
  EXPECT_TRUE(TestExpression<int32_t>(table_empty_, *Case_(1, empty_a_, empty_a_), {}));
  EXPECT_TRUE(TestExpression<int32_t>(table_empty_, *Case_(GreaterThan_(empty_a_, 3), empty_a_, empty_a_), {}));
  EXPECT_TRUE(TestExpression<int32_t>(table_empty_, *Case_(Equals_(Add_(NullValue{}, 1), 0), 1, 2), {2}));
  // clang-format on
}

}  // end namespace skyrise
