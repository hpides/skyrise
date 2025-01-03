#include "compiler/physical_query_plan/operator_proxy/alias_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class AliasOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {}

 protected:
  const std::vector<std::string> aliases_ = {"category", "revenue"};
  const std::vector<ColumnId> column_ids_ = {ColumnId{0}, ColumnId{1}};
};

TEST_F(AliasOperatorProxyTest, BaseProperties) {
  const auto alias_proxy = AliasOperatorProxy::Make(column_ids_, aliases_);
  EXPECT_EQ(alias_proxy->Type(), OperatorType::kAlias);
  EXPECT_EQ(alias_proxy->ColumnIds(), column_ids_);
  EXPECT_EQ(alias_proxy->Aliases(), aliases_);
  EXPECT_FALSE(alias_proxy->IsPipelineBreaker());
  EXPECT_EQ(alias_proxy->OutputColumnsCount(), column_ids_.size());
}

TEST_F(AliasOperatorProxyTest, Description) {
  const auto alias_proxy = AliasOperatorProxy::Make(column_ids_, aliases_);

  EXPECT_EQ(alias_proxy->Description(DescriptionMode::kSingleLine), "[Alias] category, revenue");
  EXPECT_EQ(alias_proxy->Description(DescriptionMode::kMultiLine), "[Alias]\ncategory,\nrevenue");
}

TEST_F(AliasOperatorProxyTest, SerializeAndDeserialize) {
  const auto proxy = AliasOperatorProxy::Make(column_ids_, aliases_);
  // (1) Serialize
  const auto proxy_json = proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = AliasOperatorProxy::FromJson(proxy_json);
  const auto deserialized_alias_proxy = std::dynamic_pointer_cast<AliasOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(deserialized_alias_proxy->ColumnIds(), column_ids_);
  EXPECT_EQ(deserialized_alias_proxy->Aliases(), aliases_);

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(AliasOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto alias_proxy =
  AliasOperatorProxy::Make(column_ids_, aliases_,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  const auto alias_proxy_copy = std::dynamic_pointer_cast<AliasOperatorProxy>(alias_proxy->DeepCopy());
  EXPECT_EQ(alias_proxy_copy->Aliases(), aliases_);
  EXPECT_EQ(alias_proxy_copy->ColumnIds(), column_ids_);
  EXPECT_EQ(alias_proxy_copy->InputNodeCount(), 1);
  // Without input
  alias_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(alias_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(AliasOperatorProxyTest, CreateOperatorInstance) {
  // clang-format off
  const auto alias_proxy =
  AliasOperatorProxy::Make(column_ids_, aliases_,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on                                                         
  EXPECT_TRUE(alias_proxy->GetOrCreateOperatorInstance());
}

}  // namespace skyrise
