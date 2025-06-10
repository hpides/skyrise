#include "compiler/physical_query_plan/operator_proxy/union_operator_proxy.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class UnionOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    import_proxy_a_ = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
    import_proxy_b_ = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  }

 protected:
  std::shared_ptr<ImportOperatorProxy> import_proxy_a_, import_proxy_b_;
  static inline const std::vector<ObjectReference> kObjectReferences = {ObjectReference("dummy_bucket", "key1.orc"),
                                                                        ObjectReference("dummy_bucket", "key2.orc"),
                                                                        ObjectReference("dummy_bucket", "key3.orc")};

  static inline const std::vector<ColumnId> kColumnIds = {ColumnId{0}, ColumnId{3}, ColumnId{4}};
};

TEST_F(UnionOperatorProxyTest, BaseProperties) {
  const auto union_all_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll, import_proxy_a_, import_proxy_b_);
  EXPECT_EQ(union_all_proxy->Type(), OperatorType::kUnion);
  EXPECT_TRUE(union_all_proxy->IsPipelineBreaker());
  EXPECT_EQ(union_all_proxy->OutputColumnsCount(), import_proxy_a_->OutputColumnsCount());
  EXPECT_EQ(union_all_proxy->OutputObjectsCount(), import_proxy_a_->OutputObjectsCount());
}

TEST_F(UnionOperatorProxyTest, Description) {
  const auto union_proxy = UnionOperatorProxy::Make(SetOperationMode::kUnique);
  EXPECT_EQ(union_proxy->Description(DescriptionMode::kSingleLine), "[Union] Unique");
  EXPECT_EQ(union_proxy->Description(DescriptionMode::kMultiLine), "[Union]\nUnique");

  const auto union_all_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll);
  EXPECT_EQ(union_all_proxy->Description(DescriptionMode::kSingleLine), "[Union] All");
  EXPECT_EQ(union_all_proxy->Description(DescriptionMode::kMultiLine), "[Union]\nAll");
}

TEST_F(UnionOperatorProxyTest, SerializeAndDeserialize) {
  const auto union_proxy = UnionOperatorProxy::Make(SetOperationMode::kUnique);
  const auto union_all_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll);

  // (1) Serialize
  const auto union_json = union_proxy->ToJson();
  const auto union_all_json = union_all_proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_union =
      std::dynamic_pointer_cast<UnionOperatorProxy>(UnionOperatorProxy::FromJson(union_json));
  const auto deserialized_union_all =
      std::dynamic_pointer_cast<UnionOperatorProxy>(UnionOperatorProxy::FromJson(union_all_json));
  EXPECT_EQ(deserialized_union->GetSetOperationMode(), SetOperationMode::kUnique);
  EXPECT_EQ(deserialized_union_all->GetSetOperationMode(), SetOperationMode::kAll);

  // (3) Serialize again
  const auto deserialized_union_json = deserialized_union->ToJson();
  const auto deserialized_union_all_json = deserialized_union_all->ToJson();
  EXPECT_EQ(union_json, deserialized_union_json);
  EXPECT_EQ(union_all_json, deserialized_union_all_json);
}

TEST_F(UnionOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto union_proxy =
  UnionOperatorProxy::Make(SetOperationMode::kAll,
    import_proxy_a_,
    import_proxy_b_);

  // clang-format on
  const auto union_proxy_copy = std::dynamic_pointer_cast<UnionOperatorProxy>(union_proxy->DeepCopy());
  EXPECT_EQ(union_proxy_copy->GetSetOperationMode(), SetOperationMode::kAll);
  EXPECT_EQ(union_proxy_copy->InputNodeCount(), 2);
  // Without input
  union_proxy->SetLeftInput(nullptr);
  union_proxy->SetRightInput(nullptr);
  EXPECT_EQ(union_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(UnionOperatorProxyTest, CreateOperatorInstance) {
  // TODO(tobodner): Adjust test when adding the operator implementation.
  // clang-format off
  const auto union_proxy =
  UnionOperatorProxy::Make(SetOperationMode::kUnique,
    import_proxy_a_,
    import_proxy_b_);

  // clang-format on
  EXPECT_THROW(union_proxy->GetOrCreateOperatorInstance(), std::logic_error);
}

}  // namespace skyrise
