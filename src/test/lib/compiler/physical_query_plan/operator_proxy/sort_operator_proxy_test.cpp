#include "compiler/physical_query_plan/operator_proxy/sort_operator_proxy.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class SortOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    const SortColumnDefinition sort_definition1(ColumnId{0}, SortMode::kAscending);
    const SortColumnDefinition sort_definition2(ColumnId{1}, SortMode::kDescending);
    sort_definitions_ = {sort_definition1, sort_definition2};
  }

 protected:
  std::vector<SortColumnDefinition> sort_definitions_;
};

TEST_F(SortOperatorProxyTest, BaseProperties) {
  const auto sort_proxy = SortOperatorProxy::Make(sort_definitions_);
  EXPECT_EQ(sort_proxy->Type(), OperatorType::kSort);
  EXPECT_EQ(sort_proxy->SortDefinitions(), sort_definitions_);
  EXPECT_TRUE(sort_proxy->IsPipelineBreaker());
}

TEST_F(SortOperatorProxyTest, Description) {
  const auto sort_proxy = SortOperatorProxy::Make(sort_definitions_);
  EXPECT_EQ(sort_proxy->Description(DescriptionMode::kSingleLine), "[Sort]");
  EXPECT_EQ(sort_proxy->Description(DescriptionMode::kMultiLine), "[Sort]");
}

TEST_F(SortOperatorProxyTest, SerializeAndDeserialize) {
  const auto sort_proxy = SortOperatorProxy::Make(sort_definitions_);
  // (1) Serialize
  const auto proxy_json = sort_proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = SortOperatorProxy::FromJson(proxy_json);
  const auto deserialized_sort_proxy = std::dynamic_pointer_cast<SortOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(deserialized_sort_proxy->SortDefinitions(), sort_definitions_);

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(SortOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto sort_proxy =
  SortOperatorProxy::Make(sort_definitions_,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  const auto sort_proxy_copy = std::dynamic_pointer_cast<SortOperatorProxy>(sort_proxy->DeepCopy());
  EXPECT_EQ(sort_proxy_copy->SortDefinitions(), sort_definitions_);
  EXPECT_EQ(sort_proxy_copy->InputNodeCount(), 1);
  // Without input
  sort_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(sort_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(SortOperatorProxyTest, CreateOperatorInstance) {
  // clang-format off
  const auto sort_proxy =
  SortOperatorProxy::Make(sort_definitions_, 
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  EXPECT_TRUE(sort_proxy->GetOrCreateOperatorInstance());
}

}  // namespace skyrise
