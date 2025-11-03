#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "types.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

class FilterOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
    b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
    predicate_ = GreaterThanEquals_(a_, b_);
  }

 protected:
  std::shared_ptr<AbstractExpression> a_, b_;
  std::shared_ptr<AbstractExpression> predicate_;
  static inline const std::string kBucketName = "dummy_bucket";
};

TEST_F(FilterOperatorProxyTest, BaseProperties) {
  const auto filter_proxy = FilterOperatorProxy::Make(predicate_);
  EXPECT_EQ(filter_proxy->Type(), OperatorType::kFilter);
  EXPECT_EQ(filter_proxy->Predicate(), predicate_);
  EXPECT_FALSE(filter_proxy->IsPipelineBreaker());
}

TEST_F(FilterOperatorProxyTest, Description) {
  const auto filter_proxy = FilterOperatorProxy::Make(predicate_);

  EXPECT_EQ(filter_proxy->Description(DescriptionMode::kSingleLine), "[Filter] a >= b");
  EXPECT_EQ(filter_proxy->Description(DescriptionMode::kMultiLine), "[Filter]\na >= b");
}

TEST_F(FilterOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto filter_proxy =
  FilterOperatorProxy::Make(predicate_,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference(kBucketName, "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  const auto filter_proxy_copy = std::dynamic_pointer_cast<FilterOperatorProxy>(filter_proxy->DeepCopy());
  EXPECT_NE(filter_proxy_copy->Predicate(), predicate_);
  EXPECT_EQ(*filter_proxy_copy->Predicate(), *predicate_);
  EXPECT_EQ(filter_proxy_copy->InputNodeCount(), 1);
  // Without input
  filter_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(filter_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(FilterOperatorProxyTest, SerializeAndDeserialize) {
  const auto filter_proxy = FilterOperatorProxy::Make(predicate_);
  // (1) Serialize
  const auto proxy_json = filter_proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = FilterOperatorProxy::FromJson(proxy_json);
  const auto deserialized_filter_proxy = std::dynamic_pointer_cast<FilterOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(*deserialized_filter_proxy->Predicate(), *predicate_);

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(FilterOperatorProxyTest, CreateOperatorInstance) {
  // clang-format off
  const auto filter_proxy =
  FilterOperatorProxy::Make(predicate_,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference(kBucketName, "import.orc")}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  EXPECT_TRUE(filter_proxy->GetOrCreateOperatorInstance());
}

}  // namespace skyrise
