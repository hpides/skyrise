#include "compiler/physical_query_plan/operator_proxy/aggregate_operator_proxy.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "types.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

class AggregateOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    group_by_column_ids_ = {ColumnId{0}};

    a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
    b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
    c_ = PqpColumn_(ColumnId{2}, DataType::kLong, false, "c");

    aggregates_ = ExpressionVector_(Sum_(b_), Avg_(c_));
  }

 protected:
  std::shared_ptr<PqpColumnExpression> a_, b_, c_;
  std::vector<ColumnId> group_by_column_ids_;
  std::vector<std::shared_ptr<AbstractExpression>> aggregates_;
};

TEST_F(AggregateOperatorProxyTest, BaseProperties) {
  const auto aggregate_proxy = AggregateOperatorProxy::Make(group_by_column_ids_, aggregates_);
  EXPECT_EQ(aggregate_proxy->Type(), OperatorType::kAggregate);
  EXPECT_EQ(aggregate_proxy->GroupByColumnIds(), group_by_column_ids_);
  EXPECT_EQ(aggregate_proxy->Aggregates(), aggregates_);
  EXPECT_TRUE(aggregate_proxy->IsPipelineBreaker());
  EXPECT_EQ(aggregate_proxy->OutputColumnsCount(), group_by_column_ids_.size() + aggregates_.size());
}

TEST_F(AggregateOperatorProxyTest, Description) {
  std::vector<ColumnId> group_by_column_ids = {ColumnId{0}, ColumnId{1}};
  const auto aggregates = ExpressionVector_(Sum_(c_));
  const auto aggregate_proxy = AggregateOperatorProxy::Make(group_by_column_ids, aggregates);

  EXPECT_EQ(aggregate_proxy->Description(DescriptionMode::kSingleLine), "[Aggregate] GroupByColumnIds{0, 1} SUM(c)");
  EXPECT_EQ(aggregate_proxy->Description(DescriptionMode::kMultiLine), "[Aggregate]\nGroupByColumnIds{0, 1}\nSUM(c)");
}

TEST_F(AggregateOperatorProxyTest, DescriptionMultipleAggregates) {
  const auto aggregate_proxy = AggregateOperatorProxy::Make(group_by_column_ids_, aggregates_);

  EXPECT_EQ(aggregate_proxy->Description(DescriptionMode::kSingleLine),
            "[Aggregate] GroupByColumnIds{0} SUM(b), AVG(c)");
  EXPECT_EQ(aggregate_proxy->Description(DescriptionMode::kMultiLine),
            "[Aggregate]\nGroupByColumnIds{0}\nSUM(b),\nAVG(c)");
}

TEST_F(AggregateOperatorProxyTest, SetIsPipelineBreaker) {
  const auto aggregate_proxy = AggregateOperatorProxy::Make(group_by_column_ids_, aggregates_);
  aggregate_proxy->SetIsPipelineBreaker(false);
  EXPECT_FALSE(aggregate_proxy->IsPipelineBreaker());
}

TEST_F(AggregateOperatorProxyTest, SerializeAndDeserialize) {
  const auto aggregate_proxy = AggregateOperatorProxy::Make(group_by_column_ids_, aggregates_);
  // (1) Serialize
  const auto proxy_json = aggregate_proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = AggregateOperatorProxy::FromJson(proxy_json);
  const auto deserialized_aggregate_proxy = std::dynamic_pointer_cast<AggregateOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(deserialized_aggregate_proxy->GroupByColumnIds(), group_by_column_ids_);
  EXPECT_TRUE(ExpressionsEqual(deserialized_aggregate_proxy->Aggregates(), aggregates_));
  EXPECT_TRUE(deserialized_aggregate_proxy->IsPipelineBreaker());

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(AggregateOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto aggregate_proxy =
  AggregateOperatorProxy::Make(group_by_column_ids_, aggregates_,
    ImportOperatorProxy::Make(
      std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  aggregate_proxy->SetIsPipelineBreaker(false);
  const auto aggregate_proxy_copy = std::dynamic_pointer_cast<AggregateOperatorProxy>(aggregate_proxy->DeepCopy());
  EXPECT_EQ(aggregate_proxy_copy->GroupByColumnIds(), group_by_column_ids_);
  EXPECT_NE(aggregate_proxy_copy->Aggregates(), aggregates_);
  EXPECT_TRUE(ExpressionsEqual(aggregate_proxy_copy->Aggregates(), aggregates_));
  EXPECT_FALSE(aggregate_proxy_copy->IsPipelineBreaker());
  EXPECT_EQ(aggregate_proxy_copy->InputNodeCount(), 1);
  // Without input
  aggregate_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(aggregate_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(AggregateOperatorProxyTest, CreateOperatorInstance) {
  // TODO(tobodner): Adjust test when adding the operator implementation.
  // clang-format off
  const auto aggregate_proxy =
  AggregateOperatorProxy::Make(group_by_column_ids_, aggregates_,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}, ColumnId{2}}));

  // clang-format on
  EXPECT_NE(aggregate_proxy->GetOrCreateOperatorInstance(), nullptr);
}

}  // namespace skyrise
