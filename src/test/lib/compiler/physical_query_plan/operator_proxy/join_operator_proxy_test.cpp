#include "compiler/physical_query_plan/operator_proxy/join_operator_proxy.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "operator/join_operator_predicate.hpp"
#include "types.hpp"

namespace skyrise {

class JoinOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    primary_predicate_ = std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
        .column_id_left = 0, .column_id_right = 0, .predicate_condition = PredicateCondition::kEquals});
  }

 protected:
  std::shared_ptr<JoinOperatorPredicate> primary_predicate_;
  std::vector<std::shared_ptr<JoinOperatorPredicate>> empty_secondary_predicates_;
  static inline const std::vector<ObjectReference> kObjectReferences = {ObjectReference("dummy_bucket", "key1.orc"),
                                                                        ObjectReference("dummy_bucket", "key2.orc"),
                                                                        ObjectReference("dummy_bucket", "key3.orc")};
};

TEST_F(JoinOperatorProxyTest, BaseProperties) {
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_);
  EXPECT_EQ(join_proxy->Type(), OperatorType::kHashJoin);
  EXPECT_TRUE(join_proxy->RequiresRightInput());
  EXPECT_TRUE(join_proxy->IsPipelineBreaker());
}

TEST_F(JoinOperatorProxyTest, Description) {
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_);
  join_proxy->SetImplementation(OperatorType::kHashJoin);

  EXPECT_EQ(join_proxy->Description(DescriptionMode::kSingleLine),
            "[HashJoin] Inner where column #0 kEquals column #0");
  EXPECT_EQ(join_proxy->Description(DescriptionMode::kMultiLine),
            "[HashJoin]\nInner\nwhere column #0 kEquals column #0");
}

TEST_F(JoinOperatorProxyTest, DescriptionMultiPredicate) {
  const std::vector<std::shared_ptr<JoinOperatorPredicate>> secondary_predicates{
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
          .column_id_left = 0, .column_id_right = 1, .predicate_condition = PredicateCondition::kEquals}),
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
          .column_id_left = 1, .column_id_right = 1, .predicate_condition = PredicateCondition::kEquals})};
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kFullOuter, primary_predicate_, secondary_predicates);

  EXPECT_EQ(join_proxy->Description(DescriptionMode::kSingleLine),
            "[HashJoin] Full Outer where column #0 kEquals column #0 and column #0 kEquals column #1 and column "
            "#1 kEquals column #1");
  EXPECT_EQ(join_proxy->Description(DescriptionMode::kMultiLine),
            "[HashJoin]\nFull Outer\nwhere column #0 kEquals column #0\nand column #0 kEquals column #1\nand "
            "column #1 kEquals column #1");
}

TEST_F(JoinOperatorProxyTest, DescriptionCross) {
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kCross, nullptr, empty_secondary_predicates_);
  EXPECT_EQ(join_proxy->Description(DescriptionMode::kSingleLine), "[HashJoin] Cross");
  EXPECT_EQ(join_proxy->Description(DescriptionMode::kMultiLine), "[HashJoin]\nCross");
}

TEST_F(JoinOperatorProxyTest, OutputObjectsCount) {
  const std::vector<ColumnId> column_ids = {ColumnId{0}};
  const auto import_proxy_a = ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}});
  import_proxy_a->SetOutputObjectsCount(1);
  const auto import_proxy_b = ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}});
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
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}}),
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));
  // clang-format on
  EXPECT_EQ(join_proxy->OutputColumnsCount(), 3);
}

TEST_F(JoinOperatorProxyTest, SerializeAndDeserialize) {
  const auto join_proxy = JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_);

  join_proxy->SetImplementation(OperatorType::kHashJoin);

  const auto proxy_json = join_proxy->ToJson();

  const auto deserialized_proxy = JoinOperatorProxy::FromJson(proxy_json);
  const auto deserialized_join_proxy = std::dynamic_pointer_cast<JoinOperatorProxy>(deserialized_proxy);

  EXPECT_EQ(deserialized_join_proxy->PrimaryPredicate()->column_id_left, primary_predicate_->column_id_left);
  EXPECT_EQ(deserialized_join_proxy->PrimaryPredicate()->column_id_right, primary_predicate_->column_id_right);
  EXPECT_EQ(deserialized_join_proxy->PrimaryPredicate()->predicate_condition, primary_predicate_->predicate_condition);

  EXPECT_EQ(deserialized_join_proxy->SecondaryPredicates().size(), empty_secondary_predicates_.size());
  EXPECT_EQ(deserialized_join_proxy->GetJoinMode(), JoinMode::kInner);
  EXPECT_EQ(deserialized_join_proxy->Type(), OperatorType::kHashJoin);

  const auto reserialized_proxy_json = deserialized_proxy->ToJson();

  EXPECT_EQ(proxy_json.View().WriteCompact(), reserialized_proxy_json.View().WriteCompact());
}

TEST_F(JoinOperatorProxyTest, DeepCopy) {
  const std::vector<std::shared_ptr<JoinOperatorPredicate>> secondary_predicates{
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
          .column_id_left = 0, .column_id_right = 1, .predicate_condition = PredicateCondition::kEquals}),
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{
          .column_id_left = 1, .column_id_right = 1, .predicate_condition = PredicateCondition::kEquals})};
  // clang-format off
  const auto join_proxy =
  JoinOperatorProxy::Make(JoinMode::kLeftOuter, primary_predicate_, secondary_predicates,
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}}),
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  const auto join_proxy_copy = std::dynamic_pointer_cast<JoinOperatorProxy>(join_proxy->DeepCopy());
  EXPECT_EQ(join_proxy_copy->GetJoinMode(), JoinMode::kLeftOuter);
  EXPECT_NE(join_proxy_copy->PrimaryPredicate(), primary_predicate_);
  EXPECT_NE(join_proxy_copy->SecondaryPredicates(), secondary_predicates);
  EXPECT_EQ(join_proxy_copy->InputNodeCount(), 2);
  // Without input
  join_proxy->SetLeftInput(nullptr);
  join_proxy->SetRightInput(nullptr);
  EXPECT_EQ(join_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(JoinOperatorProxyTest, CreateOperatorInstance) {
  // TODO(tobodner): Adjust test when adding the operator implementation.
  // clang-format off
  const auto join_proxy =
  JoinOperatorProxy::Make(JoinMode::kInner, primary_predicate_, empty_secondary_predicates_,
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}}),
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  join_proxy->SetImplementation(OperatorType::kHashJoin);
  EXPECT_TRUE(join_proxy->GetOrCreateOperatorInstance());
}

}  // namespace skyrise
