#include "compiler/physical_query_plan/filter_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

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
  static inline const std::string bucket_ = "dummy_bucket";
  static inline const std::vector<std::string> object_keys_{"key1", "key2"};
};

TEST_F(FilterOperatorProxyTest, BaseProperties) {
  auto filter_proxy = FilterOperatorProxy::Make(predicate_);
  EXPECT_EQ(filter_proxy->Type(), OperatorType::kFilter);
  EXPECT_EQ(filter_proxy->Predicate(), predicate_);
  EXPECT_FALSE(filter_proxy->IsPipelineBreaker());
}

TEST_F(FilterOperatorProxyTest, Description) {
  auto filter_proxy = FilterOperatorProxy::Make(predicate_);

  EXPECT_EQ(filter_proxy->Description(DescriptionMode::kSingleLine), "[Filter] a >= b");
  EXPECT_EQ(filter_proxy->Description(DescriptionMode::kMultiLine), "[Filter]\na >= b");
}

TEST_F(FilterOperatorProxyTest, DeepCopy) {
  // clang-format off
  auto filter_proxy =
  FilterOperatorProxy::Make(predicate_,
    ImportOperatorProxy::Make("bucket_name", std::vector<std::string>{"import.orc"}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  auto filter_proxy_copy = std::dynamic_pointer_cast<FilterOperatorProxy>(filter_proxy->DeepCopy());
  EXPECT_NE(filter_proxy_copy->Predicate(), predicate_);
  EXPECT_EQ(*filter_proxy_copy->Predicate(), *predicate_);
  EXPECT_EQ(filter_proxy_copy->InputNodeCount(), 1);
  // Without input
  filter_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(filter_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(FilterOperatorProxyTest, SerializeAndDeserialize) {
  auto filter_proxy = FilterOperatorProxy::Make(predicate_);
  // (1) Serialize
  auto proxy_json = filter_proxy->ToJson();

  // (2) Deserialize & verify attributes
  auto deserialized_proxy = FilterOperatorProxy::FromJson(proxy_json);
  auto deserialized_filter_proxy = std::dynamic_pointer_cast<FilterOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(*deserialized_filter_proxy->Predicate(), *predicate_);

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(FilterOperatorProxyTest, CreateOperatorInstance) {
  // TODO(anyone): Adjust test when adding the operator implementation.
  // clang-format off
  auto filter_proxy =
  FilterOperatorProxy::Make(predicate_,
    ImportOperatorProxy::Make("bucket_name", std::vector<std::string>{"import.orc"}, std::vector<ColumnId>{ColumnId{0}, ColumnId{1}}));

  // clang-format on
  EXPECT_THROW(filter_proxy->GetOrCreateOperatorInstance(), std::logic_error);
}

}  // namespace skyrise
