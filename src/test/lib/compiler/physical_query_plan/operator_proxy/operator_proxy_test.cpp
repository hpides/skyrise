#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/alias_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/union_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "types.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

class OperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    a_ = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
    b_ = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
  }

 protected:
  std::shared_ptr<AbstractExpression> a_, b_;
  static inline const std::vector<ObjectReference> kObjectReferences = {ObjectReference("dummy_bucket", "a.orc"),
                                                                        ObjectReference("dummy_bucket", "b.orc")};
  static inline const std::vector<ColumnId> kColumnIds = {ColumnId{1}, ColumnId{3}};
};

TEST_F(OperatorProxyTest, DefaultIdentity) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  std::stringstream expected_stream;
  expected_stream << import_proxy->Name() << import_proxy.get();
  EXPECT_EQ(import_proxy->Identity(), expected_stream.str());
}

TEST_F(OperatorProxyTest, SetIdentity) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy->SetIdentity("xyz123");
  EXPECT_EQ(import_proxy->Identity(), "xyz123");
}

TEST_F(OperatorProxyTest, PrefixIdentity) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy->SetIdentity("ImportXYZ");
  import_proxy->PrefixIdentity("my_prefix");

  EXPECT_EQ(import_proxy->Identity(), "my_prefix-ImportXYZ");
}

TEST_F(OperatorProxyTest, DescriptionIncludesComment) {
  const auto proxy = UnionOperatorProxy::Make(SetOperationMode::kAll);
  proxy->SetComment("dummy comment");

  EXPECT_EQ(proxy->Description(DescriptionMode::kSingleLine), "[Union] (dummy comment) All");
  EXPECT_EQ(proxy->Description(DescriptionMode::kMultiLine), "[Union]\n(dummy comment)\nAll");
}

TEST_F(OperatorProxyTest, SetLeftInput) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  ASSERT_EQ(import_proxy->OutputNodeCount(), 0);
  const auto filter_proxy = FilterOperatorProxy::Make(GreaterThanEquals_(a_, b_), import_proxy);

  EXPECT_EQ(filter_proxy->LeftInput(), import_proxy);
  EXPECT_EQ(filter_proxy->OutputNodeCount(), 0);
  EXPECT_EQ(import_proxy->OutputNodeCount(), 1);
}

TEST_F(OperatorProxyTest, SetBothInputs) {
  const auto import_proxy_a = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  const auto import_proxy_b = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  ASSERT_EQ(import_proxy_a->OutputNodeCount(), 0);
  ASSERT_EQ(import_proxy_b->OutputNodeCount(), 0);

  const auto union_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll, import_proxy_a, import_proxy_b);
  EXPECT_EQ(union_proxy->InputNodeCount(), 2);
  EXPECT_EQ(union_proxy->LeftInput(), import_proxy_a);
  EXPECT_EQ(union_proxy->RightInput(), import_proxy_b);
  EXPECT_EQ(union_proxy->OutputNodeCount(), 0);

  EXPECT_EQ(import_proxy_a->OutputNodeCount(), 1);
  EXPECT_EQ(import_proxy_b->OutputNodeCount(), 1);
}

TEST_F(OperatorProxyTest, InputObjectsCount) {
  // clang-format off
  const auto union_proxy =
  UnionOperatorProxy::Make(SetOperationMode::kAll,
    ImportOperatorProxy::Make(kObjectReferences, kColumnIds),
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference("dummy_bucket", "c.orc")}, kColumnIds));
  // clang-format on
  EXPECT_EQ(union_proxy->InputObjectsCount(), kObjectReferences.size() + 1);
}

TEST_F(OperatorProxyTest, OutputObjectsCount) {
  // clang-format off
  const auto filter_proxy =
  FilterOperatorProxy::Make(GreaterThanEquals_(a_, b_),
    ImportOperatorProxy::Make(kObjectReferences, kColumnIds));
  // clang-format on
  EXPECT_EQ(filter_proxy->OutputObjectsCount(), kObjectReferences.size());
}

TEST_F(OperatorProxyTest, OutputColumnsCount) {
  // clang-format off
  const auto filter_proxy =
  FilterOperatorProxy::Make(GreaterThanEquals_(a_, b_),
    ImportOperatorProxy::Make(kObjectReferences, std::vector<ColumnId>{{ColumnId{0}, ColumnId{1}, ColumnId{5}}}));
  // clang-format on
  EXPECT_EQ(filter_proxy->OutputColumnsCount(), 3);
}

TEST_F(OperatorProxyTest, SerializeInputsAsPlaceholders) {
  const auto import_proxy_a = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  const auto import_proxy_b = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  const auto union_all_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll, import_proxy_a, import_proxy_b);

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
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy->SetIdentity("test123");
  import_proxy->SetComment("my-comment");

  const auto import_proxy_copy = import_proxy->DeepCopy();
  EXPECT_EQ(import_proxy->Identity(), import_proxy_copy->Identity());
  EXPECT_EQ(import_proxy->Comment(), import_proxy_copy->Comment());
}

TEST_F(OperatorProxyTest, DeepCopyDiamondShape) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  const auto a = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
  const auto b = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
  // clang-format off
  const auto pqp =
  UnionOperatorProxy::Make(SetOperationMode::kAll,
    FilterOperatorProxy::Make(GreaterThan_(a, b),
      import_proxy),
    FilterOperatorProxy::Make(LessThan_(a, b),
      import_proxy));
  // clang-format on

  const auto copied_pqp = pqp->DeepCopy();
  EXPECT_EQ(copied_pqp->LeftInput()->LeftInput(), copied_pqp->RightInput()->LeftInput());
}

TEST_F(OperatorProxyTest, StreamOperator) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  const auto a = PqpColumn_(ColumnId{0}, DataType::kLong, false, "a");
  const auto b = PqpColumn_(ColumnId{1}, DataType::kLong, false, "b");
  // clang-format off
  const auto pqp =
  UnionOperatorProxy::Make(SetOperationMode::kAll,
    FilterOperatorProxy::Make(GreaterThan_(a, b),
      import_proxy),
    FilterOperatorProxy::Make(LessThan_(a, b),
      import_proxy));

  // Please note that backslashes must be escaped in strings.
  const std::string expected_output =
  "[0] [Union] All\n"
  " \\_[1] [Filter] a > b\n"
  " |  \\_[2] [Import] dummy_bucket/{2 objects} ColumnIds{1, 3}\n"
  " \\_[3] [Filter] a < b\n"
  "    \\_Recurring Node --> [2]\n";
  // clang-format on

  std::stringstream stream;
  stream << *pqp;
  EXPECT_EQ(stream.str(), expected_output);
}

}  // namespace skyrise
