#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/optimizer/strategy/pqp_pipeline_preparation_rule.hpp"
#include "compiler/physical_query_plan/aggregate_operator_proxy.hpp"
#include "compiler/physical_query_plan/exchange_operator_proxy.hpp"
#include "compiler/physical_query_plan/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/sort_operator_proxy.hpp"
#include "compiler/physical_query_plan/union_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class PqpPipelinePreparationRuleTest : public ::testing::Test {
 public:
  void SetUp() override {
    rule_ = std::make_unique<PqpPipelinePreparationRule>();

    const std::vector<std::string> object_keys{"partition1", "partition2", "partition3"};
    import_proxy_a_ = ImportOperatorProxy::Make(bucket_name_, object_keys, column_ids_);
    import_proxy_b_ = ImportOperatorProxy::Make(bucket_name_, object_keys, column_ids_);

    a_a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a_a");
    a_b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "a_b");
    b_x_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "b_x");
    b_y_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b_y");

    SortColumnDefinition sort_definition1{ColumnId{0}, SortMode::kAscending};
    SortColumnDefinition sort_definition2{ColumnId{1}, SortMode::kAscending};
    sort_definitions_ = {sort_definition1, sort_definition2};
  }

 protected:
  static inline const std::string bucket_name_ = "dummy_bucket";
  static inline const std::vector<ColumnId> column_ids_ = {ColumnId{2}, ColumnId{4}};
  static inline const std::string target_object_key_ = "dummy_target_object_key";
  static inline const auto export_format_ = ExportFormat::kOrc;

  std::unique_ptr<PqpPipelinePreparationRule> rule_;

  std::shared_ptr<ImportOperatorProxy> import_proxy_a_;
  std::shared_ptr<ImportOperatorProxy> import_proxy_b_;

  std::shared_ptr<PqpColumnExpression> a_a_;
  std::shared_ptr<PqpColumnExpression> a_b_;
  std::shared_ptr<PqpColumnExpression> b_x_;
  std::shared_ptr<PqpColumnExpression> b_y_;

  std::vector<SortColumnDefinition> sort_definitions_;
};

TEST_F(PqpPipelinePreparationRuleTest, AggregateShuffle) {
  std::vector<ColumnId> group_by_column_ids = {ColumnId(1)};
  auto aggregates = ExpressionVector_(Sum_(a_a_));
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_,
    AggregateOperatorProxy::Make(group_by_column_ids, aggregates,
      FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
        import_proxy_a_)));
  // clang-format on

  rule_->ApplyTo(pqp);

  // PartitionOperatorProxy should be placed beneath the AggregateOperatorProxy
  EXPECT_EQ(pqp->Type(), OperatorType::kExport);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kAggregate);
  ASSERT_EQ(pqp->LeftInput()->LeftInput()->Type(), OperatorType::kExchange);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kFilter);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kImport);

  auto exchange_proxy = std::dynamic_pointer_cast<ExchangeOperatorProxy>(pqp->LeftInput()->LeftInput());
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kFullMerge);
  EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 1);
}

TEST_F(PqpPipelinePreparationRuleTest, AggregateNoShuffle) {
  // Data shuffling is not necessary when there is only one input object.
  std::vector<std::string> object_keys = {"single_partition"};

  std::vector<ColumnId> group_by_column_ids = {ColumnId(1)};
  auto aggregates = ExpressionVector_(Sum_(a_a_));
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_,
    AggregateOperatorProxy::Make(group_by_column_ids, aggregates,
      FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
        ImportOperatorProxy::Make(bucket_name_, object_keys, column_ids_))));
  // clang-format on

  rule_->ApplyTo(pqp);

  // PQP should not change.
  EXPECT_EQ(pqp->Type(), OperatorType::kExport);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kAggregate);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->Type(), OperatorType::kFilter);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kImport);
}

TEST_F(PqpPipelinePreparationRuleTest, SortShuffle) {
  std::vector<ColumnId> group_by_column_ids = {ColumnId(1)};
  auto aggregates = ExpressionVector_(Sum_(a_a_));
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_,
    SortOperatorProxy::Make(sort_definitions_,
      FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
        import_proxy_a_)));
  // clang-format on

  rule_->ApplyTo(pqp);

  // PartitionOperatorProxy should be placed beneath the SortOperatorProxy
  EXPECT_EQ(pqp->Type(), OperatorType::kExport);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kSort);
  ASSERT_EQ(pqp->LeftInput()->LeftInput()->Type(), OperatorType::kExchange);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kFilter);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kImport);

  auto exchange_proxy = std::dynamic_pointer_cast<ExchangeOperatorProxy>(pqp->LeftInput()->LeftInput());
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kFullMerge);
  EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 1);
}

TEST_F(PqpPipelinePreparationRuleTest, SortAggregateSingleShuffle) {
  std::vector<ColumnId> group_by_column_ids = {ColumnId(1)};
  auto aggregates = ExpressionVector_(Sum_(a_a_));
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_,
    SortOperatorProxy::Make(sort_definitions_,
      AggregateOperatorProxy::Make(group_by_column_ids, aggregates,
        FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
          import_proxy_a_))));
  // clang-format on

  rule_->ApplyTo(pqp);

  // A PartitionOperatorProxy should be inserted beneath AggregateOperatorProxy only. The sort operation should receive
  // a single data partition from the aggregation. Therefore, no further shuffling is required.
  EXPECT_EQ(pqp->Type(), OperatorType::kExport);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kSort);
  ASSERT_EQ(pqp->LeftInput()->LeftInput()->Type(), OperatorType::kAggregate);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kExchange);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kFilter);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kImport);

  auto exchange_proxy =
      std::dynamic_pointer_cast<ExchangeOperatorProxy>(pqp->LeftInput()->LeftInput()->LeftInput());
  EXPECT_EQ(exchange_proxy->GetExchangeMode(), ExchangeMode::kFullMerge);
  EXPECT_EQ(exchange_proxy->OutputObjectsCount(), 1);
}

TEST_F(PqpPipelinePreparationRuleTest, UnionShuffle) {
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_,
    UnionOperatorProxy::Make(SetOperationMode::kAll,
      FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
        import_proxy_a_),
      FilterOperatorProxy::Make(LessThan_(a_b_, 123),
        import_proxy_a_->DeepCopy()))); // TODO(julianmenzler): Remove DeepCopy() when the rule can do this itself
  // clang-format on

  rule_->ApplyTo(pqp);

  EXPECT_EQ(pqp->Type(), OperatorType::kExport);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kUnion);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->Type(), OperatorType::kExchange);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kFilter);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kImport);
  EXPECT_EQ(pqp->LeftInput()->RightInput()->Type(), OperatorType::kExchange);
  EXPECT_EQ(pqp->LeftInput()->RightInput()->LeftInput()->Type(), OperatorType::kFilter);
  EXPECT_EQ(pqp->LeftInput()->RightInput()->LeftInput()->LeftInput()->Type(), OperatorType::kImport);
}

TEST_F(PqpPipelinePreparationRuleTest, UnionSingleShuffle) {
  // If there are no more operators between the Import and the pipeline breaker, data shuffling has no effect and should
  // be avoided.
  // clang-format off
  auto pqp =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_,
    UnionOperatorProxy::Make(SetOperationMode::kAll,
      FilterOperatorProxy::Make(GreaterThan_(a_a_, 700),
        import_proxy_a_),
      import_proxy_a_->DeepCopy()));  // TODO(julianmenzler): Remove DeepCopy() when the rule can do this itself
  // clang-format on

  rule_->ApplyTo(pqp);

  EXPECT_EQ(pqp->Type(), OperatorType::kExport);
  EXPECT_EQ(pqp->LeftInput()->Type(), OperatorType::kUnion);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->Type(), OperatorType::kExchange);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kFilter);
  EXPECT_EQ(pqp->LeftInput()->LeftInput()->LeftInput()->LeftInput()->Type(), OperatorType::kImport);
  EXPECT_EQ(pqp->LeftInput()->RightInput()->Type(), OperatorType::kImport);
}

}  // namespace skyrise
