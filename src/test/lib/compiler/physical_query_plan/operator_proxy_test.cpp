#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/alias_operator_proxy.hpp"
#include "compiler/physical_query_plan/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/union_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class OperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
    b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
  }

 protected:
  std::shared_ptr<AbstractExpression> a_, b_;
  static inline const std::string bucket_ = "dummy_bucket";
  static inline const std::vector<std::string> object_keys_ = {"a.orc", "b.orc"};
  static inline const std::vector<ColumnId> column_ids_ = {ColumnId{1}, ColumnId{3}};
};

TEST_F(OperatorProxyTest, DefaultIdentity) {
  auto import_proxy = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  std::stringstream expected_stream;
  expected_stream << import_proxy->Name() << import_proxy.get();
  EXPECT_EQ(import_proxy->Identity(), expected_stream.str());
}

TEST_F(OperatorProxyTest, SetIdentity) {
  auto import_proxy = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  import_proxy->SetIdentity("xyz123");
  EXPECT_EQ(import_proxy->Identity(), "xyz123");
}

TEST_F(OperatorProxyTest, PrefixIdentity) {
  auto import_proxy = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  import_proxy->SetIdentity("ImportXYZ");
  import_proxy->PrefixIdentity("my_prefix_");

  EXPECT_EQ(import_proxy->Identity(), "my_prefix_ImportXYZ");
}

TEST_F(OperatorProxyTest, DescriptionIncludesComment) {
  auto proxy = UnionOperatorProxy::Make(SetOperationMode::kAll);
  proxy->SetComment("dummy comment");

  EXPECT_EQ(proxy->Description(DescriptionMode::kSingleLine), "[Union] (dummy comment) All");
  EXPECT_EQ(proxy->Description(DescriptionMode::kMultiLine), "[Union]\n(dummy comment)\nAll");
}

TEST_F(OperatorProxyTest, SetLeftInput) {
  auto import_proxy = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  ASSERT_EQ(import_proxy->OutputNodeCount(), 0);
  auto filter_proxy = FilterOperatorProxy::Make(GreaterThanEquals_(a_, b_), import_proxy);

  EXPECT_EQ(filter_proxy->LeftInput(), import_proxy);
  EXPECT_EQ(filter_proxy->OutputNodeCount(), 0);
  EXPECT_EQ(import_proxy->OutputNodeCount(), 1);
}

TEST_F(OperatorProxyTest, SetBothInputs) {
  auto import_proxy_a = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  auto import_proxy_b = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  ASSERT_EQ(import_proxy_a->OutputNodeCount(), 0);
  ASSERT_EQ(import_proxy_b->OutputNodeCount(), 0);

  auto union_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll, import_proxy_a, import_proxy_b);
  EXPECT_EQ(union_proxy->InputNodeCount(), 2);
  EXPECT_EQ(union_proxy->LeftInput(), import_proxy_a);
  EXPECT_EQ(union_proxy->RightInput(), import_proxy_b);
  EXPECT_EQ(union_proxy->OutputNodeCount(), 0);

  EXPECT_EQ(import_proxy_a->OutputNodeCount(), 1);
  EXPECT_EQ(import_proxy_b->OutputNodeCount(), 1);
}

TEST_F(OperatorProxyTest, InputObjectsCount) {
  // clang-format off
  auto union_proxy =
  UnionOperatorProxy::Make(SetOperationMode::kAll,
    ImportOperatorProxy::Make(bucket_, std::vector<std::string>{"a.orc", "b.orc"}, column_ids_),
    ImportOperatorProxy::Make(bucket_, std::vector<std::string>{"c.orc"}, column_ids_));
  // clang-format on
  EXPECT_EQ(union_proxy->InputObjectsCount(), 3);
}

TEST_F(OperatorProxyTest, OutputObjectsCount) {
  // clang-format off
  auto filter_proxy =
  FilterOperatorProxy::Make(GreaterThanEquals_(a_, b_),
    ImportOperatorProxy::Make(bucket_, std::vector<std::string>{"a.orc", "b.orc"}, column_ids_));
  // clang-format on
  EXPECT_EQ(filter_proxy->OutputObjectsCount(), 2);
}

TEST_F(OperatorProxyTest, OutputColumnsCount) {
  // clang-format off
  auto filter_proxy =
  FilterOperatorProxy::Make(GreaterThanEquals_(a_, b_),
    ImportOperatorProxy::Make(bucket_, object_keys_, std::vector<ColumnId>{{ColumnId{0}, ColumnId{1}, ColumnId{5}}}));
  // clang-format on
  EXPECT_EQ(filter_proxy->OutputColumnsCount(), 3);
}

TEST_F(OperatorProxyTest, SerializeInputsAsPlaceholders) {
  auto import_proxy_a = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  auto import_proxy_b = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  auto union_all_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll, import_proxy_a, import_proxy_b);

  ASSERT_NO_THROW(import_proxy_a->Identity());
  ASSERT_NO_THROW(import_proxy_b->Identity());
  ASSERT_NO_THROW(union_all_proxy->Identity());
  ASSERT_NE(import_proxy_a->Identity(), import_proxy_b->Identity());

  // Serialize operator proxy
  const Aws::Utils::Json::JsonValue json = union_all_proxy->ToJson();

  EXPECT_EQ(json.View().GetString("left_input_operator_identity"), import_proxy_a->Identity());
  EXPECT_EQ(json.View().GetString("right_input_operator_identity"), import_proxy_b->Identity());
}

TEST_F(OperatorProxyTest, DeepCopy) {
  const auto import_proxy = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  import_proxy->SetIdentity("test123");
  import_proxy->SetComment("my-comment");

  const auto import_proxy_copy = import_proxy->DeepCopy();
  EXPECT_EQ(import_proxy->Identity(), import_proxy_copy->Identity());
  EXPECT_EQ(import_proxy->Comment(), import_proxy_copy->Comment());
}

TEST_F(OperatorProxyTest, DeepCopyDiamondShape) {
  const auto import_proxy = ImportOperatorProxy::Make(bucket_, object_keys_, column_ids_);
  const auto a = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
  const auto b = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
  const auto filter_proxy_a = FilterOperatorProxy::Make(GreaterThan_(a, b), import_proxy);
  const auto filter_proxy_b = FilterOperatorProxy::Make(LessThan_(a, b), import_proxy);
  const auto union_all_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll, filter_proxy_a, filter_proxy_b);

  const auto copied_pqp = union_all_proxy->DeepCopy();

  EXPECT_EQ(copied_pqp->LeftInput()->LeftInput(), copied_pqp->RightInput()->LeftInput());
}

}  // namespace skyrise
