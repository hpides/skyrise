#include "compiler/physical_query_plan/join_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class JoinOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
    b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
    x_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "x");
    y_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "y");

    primary_predicate_ = Equals_(a_, x_);
    empty_secondary_predicates_ = {};
  }

 protected:
  std::shared_ptr<PqpColumnExpression> a_, b_, x_, y_;
  std::shared_ptr<AbstractExpression> primary_predicate_;
  std::vector<std::shared_ptr<AbstractExpression>> empty_secondary_predicates_;
  static inline const std::string kBucketName = "dummy_bucket";
  static inline const std::vector<std::string> kObjectKeys = {"key1.orc", "key2.orc", "key3.orc"};
};

TEST_F(JoinOperatorProxyTest, BaseProperties) {
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_);
  EXPECT_EQ(join_proxy->Type(), OperatorType::kNestedLoopJoin);
  EXPECT_TRUE(join_proxy->RequiresRightInput());
  EXPECT_TRUE(join_proxy->IsPipelineBreaker());
}

TEST_F(JoinOperatorProxyTest, Description) {
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_);
  join_proxy->SetImplementation(OperatorType::kHashJoin);

  EXPECT_EQ(join_proxy->Description(DescriptionMode::kSingleLine), "[HashJoin] Inner where a = x");
  EXPECT_EQ(join_proxy->Description(DescriptionMode::kMultiLine), "[HashJoin]\nInner\nwhere a = x");
}

TEST_F(JoinOperatorProxyTest, DescriptionMultiPredicate) {
  const auto secondary_predicates = ExpressionVector_(NotEquals_(b_, y_), GreaterThanEquals_(b_, y_));
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kFullOuter, primary_predicate_, secondary_predicates);

  EXPECT_EQ(join_proxy->Description(DescriptionMode::kSingleLine),
            "[NestedLoopJoin] Full Outer where a = x and b != y and b >= y");
  EXPECT_EQ(join_proxy->Description(DescriptionMode::kMultiLine),
            "[NestedLoopJoin]\nFull Outer\nwhere a = x\nand b != y\nand b >= y");
}

TEST_F(JoinOperatorProxyTest, DescriptionCross) {
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kCross, nullptr, empty_secondary_predicates_);
  EXPECT_EQ(join_proxy->Description(DescriptionMode::kSingleLine), "[NestedLoopJoin] Cross");
  EXPECT_EQ(join_proxy->Description(DescriptionMode::kMultiLine), "[NestedLoopJoin]\nCross");
}

TEST_F(JoinOperatorProxyTest, OutputObjectsCount) {
  const std::vector<ColumnId> column_ids = {ColumnId{0}};
  const auto import_proxy_a = ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}});
  import_proxy_a->SetOutputObjectsCount(1);
  const auto import_proxy_b = ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}});
  import_proxy_b->SetOutputObjectsCount(2);
  // clang-format off

  const auto join_proxy =
  JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_,
    import_proxy_a,
    import_proxy_b);

  // clang-format on
  EXPECT_EQ(join_proxy->OutputObjectsCount(), 2);
}

TEST_F(JoinOperatorProxyTest, OutputColumnsCount) {
  // clang-format off
  const auto join_proxy =
  JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_,
    ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}}),
    ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));
  // clang-format on
  EXPECT_EQ(join_proxy->OutputColumnsCount(), 3);
}

TEST_F(JoinOperatorProxyTest, SerializeAndDeserialize) {
  // TODO(d-justen): Add test as part of #560 Add HashJoinOperatorProxy
  // clang-format off
  const auto join_proxy =
  JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_,
    ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}}),
    ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));
  // clang-format on

  EXPECT_THROW(join_proxy->ToJson(), std::logic_error);
  Aws::Utils::Json::JsonValue json;
  json.WithString(kJsonKeyOperatorType, std::string(magic_enum::enum_name(OperatorType::kHashJoin)));
  EXPECT_THROW(JoinOperatorProxy::FromJson(json), std::logic_error);
}

TEST_F(JoinOperatorProxyTest, DeepCopy) {
  const auto secondary_predicates = ExpressionVector_(NotEquals_(b_, y_), GreaterThanEquals_(b_, y_));
  // clang-format off
  const auto join_proxy =
  JoinOperatorProxy::Make(JoinMode::kLeftOuter, primary_predicate_, secondary_predicates,
    ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}}),
    ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  const auto join_proxy_copy = std::dynamic_pointer_cast<JoinOperatorProxy>(join_proxy->DeepCopy());
  EXPECT_EQ(join_proxy_copy->GetJoinMode(), JoinMode::kLeftOuter);
  EXPECT_NE(join_proxy_copy->PrimaryPredicate(), primary_predicate_);
  EXPECT_EQ(*join_proxy_copy->PrimaryPredicate(), *primary_predicate_);
  EXPECT_NE(join_proxy_copy->SecondaryPredicates(), secondary_predicates);
  EXPECT_TRUE(ExpressionsEqual(join_proxy_copy->SecondaryPredicates(), secondary_predicates));
  EXPECT_EQ(join_proxy_copy->InputNodeCount(), 2);
  // Without input
  join_proxy->SetLeftInput(nullptr);
  join_proxy->SetRightInput(nullptr);
  EXPECT_EQ(join_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(JoinOperatorProxyTest, CreateOperatorInstance) {
  // TODO(anyone): Adjust test when adding the operator implementation.
  // clang-format off
  const auto join_proxy =
  JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_,
    ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}}),
    ImportOperatorProxy::Make(kBucketName, kObjectKeys, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  EXPECT_THROW(join_proxy->GetOrCreateOperatorInstance(), std::logic_error);
}

}  // namespace skyrise
