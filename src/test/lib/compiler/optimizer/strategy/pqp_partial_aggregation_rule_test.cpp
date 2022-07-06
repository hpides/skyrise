#include "compiler/optimizer/strategy/pqp_partial_aggregation_rule.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/optimizer/strategy/strategy_base_test.hpp"
#include "compiler/physical_query_plan/aggregate_operator_proxy.hpp"
#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class PqpPartialAggregationRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    rule_ = std::make_shared<PqpPartialAggregationRule>();

    std::vector<ColumnId> column_ids = {ColumnId{0}, ColumnId{1}, ColumnId{2}, ColumnId{3}};
    const std::vector<std::string> object_keys{"partition1", "partition2", "partition3"};
    import_proxy_ = ImportOperatorProxy::Make("dummy_bucket", object_keys, column_ids);

    a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
    b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
    c_ = PqpColumn_(ColumnId{2}, DataType::kLong, false, "c");
    d_ = PqpColumn_(ColumnId{3}, DataType::kFloat, false, "d");
  }

  static void VerifyAggregate(const std::shared_ptr<AbstractExpression>& expression,
                              const AggregateFunction expected_function, const ColumnId expected_column_id,
                              const DataType expected_data_type, const std::string& expected_column_name) {
    ASSERT_EQ(expression->type_, ExpressionType::kAggregate);
    const auto aggregate_expression = std::static_pointer_cast<AggregateExpression>(expression);

    EXPECT_EQ(aggregate_expression->aggregate_function_, expected_function);
    ASSERT_EQ(aggregate_expression->Argument()->type_, ExpressionType::kPqpColumn);
    const auto argument_pqp_expression =
        std::static_pointer_cast<PqpColumnExpression>(aggregate_expression->Argument());
    EXPECT_EQ(argument_pqp_expression->column_id_, expected_column_id);
    EXPECT_EQ(argument_pqp_expression->data_type_, expected_data_type);
    EXPECT_EQ(argument_pqp_expression->column_name_, expected_column_name);
  }

 protected:
  std::shared_ptr<AbstractRule> rule_;
  std::shared_ptr<ImportOperatorProxy> import_proxy_;
  std::shared_ptr<PqpColumnExpression> a_;
  std::shared_ptr<PqpColumnExpression> b_;
  std::shared_ptr<PqpColumnExpression> c_;
  std::shared_ptr<PqpColumnExpression> d_;
};

TEST_F(PqpPartialAggregationRuleTest, PartialAggregation) {
  const std::vector<ColumnId> groupby_column_ids = {ColumnId{1}, ColumnId{2}};
  const auto aggregates = ExpressionVector_(Sum_(d_), Min_(a_));
  // clang-format off
  auto pqp =
  AggregateOperatorProxy::Make(groupby_column_ids, aggregates,
    import_proxy_);
  // clang-format on

  StrategyBaseTest::ApplyRule(rule_, pqp);

  EXPECT_EQ(pqp->Type(), OperatorType::kAggregate);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kAggregate);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->Type(), OperatorType::kImport);
  {
    auto pre_aggregate_proxy = std::static_pointer_cast<AggregateOperatorProxy>(pqp->LeftInput());
    EXPECT_EQ(pre_aggregate_proxy->Comment(), "Pre-Aggregate");
    EXPECT_FALSE(pre_aggregate_proxy->IsPipelineBreaker());

    EXPECT_EQ(pre_aggregate_proxy->GroupByColumnIds(), groupby_column_ids);
    EXPECT_TRUE(ExpressionsEqual(pre_aggregate_proxy->Aggregates(), aggregates));

    VerifyAggregate(pre_aggregate_proxy->Aggregates().at(0), AggregateFunction::kSum, ColumnId{3}, DataType::kFloat, "d");
    VerifyAggregate(pre_aggregate_proxy->Aggregates().at(1), AggregateFunction::kMin, ColumnId{0}, a_->data_type_, "a");
  }
  {
    auto aggregate_proxy = std::static_pointer_cast<AggregateOperatorProxy>(pqp);
    EXPECT_TRUE(aggregate_proxy->Comment().empty());
    EXPECT_TRUE(aggregate_proxy->IsPipelineBreaker());

    EXPECT_EQ(aggregate_proxy->GroupByColumnIds(), std::vector<ColumnId>({ColumnId{0}, ColumnId{1}}));
    EXPECT_FALSE(ExpressionsEqual(aggregate_proxy->Aggregates(), aggregates));

    // After SUM aggregations, DataType::kFloat input columns become DataType::kDouble aggregates, c.f. AggregateTraits.
    VerifyAggregate(aggregate_proxy->Aggregates().at(0), AggregateFunction::kSum, ColumnId{2}, DataType::kDouble ,"d");
    VerifyAggregate(aggregate_proxy->Aggregates().at(1), AggregateFunction::kMin, ColumnId{3}, a_->data_type_ ,"a");
  }
}

TEST_F(PqpPartialAggregationRuleTest, PartialAggregationCounts) {
  // COUNTs become SUMs in the final aggregation
  const std::vector<ColumnId> groupby_column_ids = {ColumnId{2}, ColumnId{3}};
  const auto aggregates = ExpressionVector_(CountStarPqp_(), Min_(a_), Count_(b_));
  // clang-format off
  auto pqp =
  AggregateOperatorProxy::Make(groupby_column_ids, aggregates,
    import_proxy_);
  // clang-format on

  StrategyBaseTest::ApplyRule(rule_, pqp);

  EXPECT_EQ(pqp->Type(), OperatorType::kAggregate);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kAggregate);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->Type(), OperatorType::kImport);
  {
    auto pre_aggregate_proxy = std::static_pointer_cast<AggregateOperatorProxy>(pqp->LeftInput());
    EXPECT_EQ(pre_aggregate_proxy->GroupByColumnIds(), groupby_column_ids);
    EXPECT_TRUE(ExpressionsEqual(pre_aggregate_proxy->Aggregates(), aggregates));

    VerifyAggregate(pre_aggregate_proxy->Aggregates().at(0), AggregateFunction::kCount, ColumnId{kInvalidColumnId},
                    DataType::kLong, "*");
    VerifyAggregate(pre_aggregate_proxy->Aggregates().at(1), AggregateFunction::kMin, ColumnId{0}, a_->data_type_,"a");
    VerifyAggregate(pre_aggregate_proxy->Aggregates().at(2), AggregateFunction::kCount, ColumnId{1}, b_->data_type_ ,"b");
  }
  {
    auto aggregate_proxy = std::static_pointer_cast<AggregateOperatorProxy>(pqp);
    EXPECT_EQ(aggregate_proxy->GroupByColumnIds(), std::vector<ColumnId>({ColumnId{0}, ColumnId{1}}));
    EXPECT_FALSE(ExpressionsEqual(aggregate_proxy->Aggregates(), aggregates));

    VerifyAggregate(aggregate_proxy->Aggregates().at(0), AggregateFunction::kSum, ColumnId{2}, DataType::kLong, "*");
    VerifyAggregate(aggregate_proxy->Aggregates().at(1), AggregateFunction::kMin, ColumnId{3}, a_->data_type_ ,"a");
    VerifyAggregate(aggregate_proxy->Aggregates().at(2), AggregateFunction::kSum, ColumnId{4}, b_->data_type_,"b");
  }
}

TEST_F(PqpPartialAggregationRuleTest, PartialAggregationCountDistinct) {
  // The rule should not touch aggregates involving unsupported AggregateExpressions, such as AVG or COUNT DISTINCT.
  const std::vector<ColumnId> groupby_column_ids = {ColumnId{1}, ColumnId{2}};
  const auto aggregates = ExpressionVector_(CountDistinct_(a_), Sum_(d_));
  // clang-format off
  auto pqp =
  AggregateOperatorProxy::Make(groupby_column_ids, aggregates,
    import_proxy_);
  // clang-format on

  StrategyBaseTest::ApplyRule(rule_, pqp);

  EXPECT_EQ(pqp->Type(), OperatorType::kAggregate);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kImport);

  auto aggregate_proxy = std::static_pointer_cast<AggregateOperatorProxy>(pqp);
  EXPECT_EQ(aggregate_proxy->GroupByColumnIds(), groupby_column_ids);
  EXPECT_TRUE(ExpressionsEqual(aggregate_proxy->Aggregates(), aggregates));
}

}  // namespace skyrise
