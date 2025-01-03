#include "compiler/physical_query_plan/operator_proxy/limit_operator_proxy.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class LimitOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {}
};

TEST_F(LimitOperatorProxyTest, BaseProperties) {
  const auto limit_proxy = LimitOperatorProxy::Make(Value_(10));
  EXPECT_EQ(limit_proxy->Type(), OperatorType::kLimit);
  EXPECT_EQ(*limit_proxy->RowCount(), *Value_(10));
  EXPECT_TRUE(limit_proxy->IsPipelineBreaker());
}

TEST_F(LimitOperatorProxyTest, DescriptionLimitOperatorProxy) {
  const auto limit_proxy = LimitOperatorProxy::Make(Value_(10));

  EXPECT_EQ(limit_proxy->Description(DescriptionMode::kSingleLine), "[Limit] 10 row(s)");
  EXPECT_EQ(limit_proxy->Description(DescriptionMode::kMultiLine), "[Limit]\n10 row(s)");
}

TEST_F(LimitOperatorProxyTest, SerializeAndDeserialize) {
  const auto row_count = Value_(100);
  const auto limit_proxy = LimitOperatorProxy::Make(row_count);

  // (1) Serialize
  const auto proxy_json = limit_proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = LimitOperatorProxy::FromJson(proxy_json);
  const auto deserialized_limit_proxy = std::dynamic_pointer_cast<LimitOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(*deserialized_limit_proxy->RowCount(), *row_count);

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(LimitOperatorProxyTest, DeepCopy) {
  const auto row_count = Add_(Value_(1), Value_(2));
  // clang-format off
  const auto limit_proxy =
  LimitOperatorProxy::Make(row_count,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  const auto limit_proxy_copy = std::dynamic_pointer_cast<LimitOperatorProxy>(limit_proxy->DeepCopy());
  EXPECT_NE(limit_proxy_copy->RowCount(), row_count);
  EXPECT_EQ(*limit_proxy_copy->RowCount(), *row_count);
  EXPECT_EQ(limit_proxy_copy->InputNodeCount(), 1);
  // Without input
  limit_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(limit_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(LimitOperatorProxyTest, CreateOperatorInstance) {
  // TODO(tobodner): Adjust test when adding the operator implementation.
  // clang-format off
  const auto limit_proxy =
  LimitOperatorProxy::Make(Value_(100),
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("bucket_name", "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  EXPECT_THROW(limit_proxy->GetOrCreateOperatorInstance(), std::logic_error);
}

}  // namespace skyrise
