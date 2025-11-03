#include "compiler/physical_query_plan/pqp_serialization.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/union_operator_proxy.hpp"
#include "expression/expression_functional.hpp"
#include "types.hpp"

namespace skyrise {

using namespace skyrise::expression_functional;  // NOLINT(google-build-using-namespace)

class PqpSerializationTest : public ::testing::Test {
 protected:
  const ObjectReference kTargetObject{"test_bucket", "target_key"};
  const std::vector<ObjectReference> kImportObjects = {ObjectReference("import_bucket", "a.orc"),
                                                       ObjectReference("import_bucket", "b.orc"),
                                                       ObjectReference("import_bucket", "c.orc")};
  const std::vector<ColumnId> kImportColumnIds = {ColumnId{1}, ColumnId{3}};
};

TEST_F(PqpSerializationTest, SingleOperatorProxy) {
  const auto comment = "This is a test comment";
  const auto import_proxy = ImportOperatorProxy::Make(kImportObjects, kImportColumnIds);
  import_proxy->SetComment(comment);

  std::string serialized_proxy = SerializePqp(import_proxy);
  const auto deserialized_proxy = DeserializePqp(serialized_proxy);

  const auto deserialized_import_proxy = std::dynamic_pointer_cast<ImportOperatorProxy>(deserialized_proxy);
  ASSERT_TRUE(deserialized_import_proxy);
  EXPECT_EQ(deserialized_import_proxy->InputNodeCount(), 0);
  EXPECT_EQ(deserialized_import_proxy->Comment(), comment);

  EXPECT_EQ(serialized_proxy, SerializePqp(deserialized_proxy));
}

TEST_F(PqpSerializationTest, LinearOperatorChain) {
  const auto import_proxy = ImportOperatorProxy::Make(kImportObjects, kImportColumnIds);
  const auto filter_proxy = FilterOperatorProxy::Make(
      GreaterThanEquals_(PqpColumn_(ColumnId{0}, DataType::kLong, false, "a"), 100), import_proxy);
  const auto export_proxy = ExportOperatorProxy::Make(kTargetObject, ExportFormat::kOrc, filter_proxy);

  std::string serialized_proxy = SerializePqp(export_proxy);
  const auto deserialized_proxy = DeserializePqp(serialized_proxy);

  const auto deserialized_export_proxy = std::dynamic_pointer_cast<ExportOperatorProxy>(deserialized_proxy);
  ASSERT_TRUE(deserialized_export_proxy);
  ASSERT_EQ(deserialized_export_proxy->InputNodeCount(), 1);

  const auto deserialized_filter_proxy =
      std::dynamic_pointer_cast<FilterOperatorProxy>(deserialized_export_proxy->LeftInput());
  ASSERT_TRUE(deserialized_filter_proxy);
  ASSERT_EQ(deserialized_filter_proxy->InputNodeCount(), 1);

  const auto deserialized_import_proxy =
      std::dynamic_pointer_cast<ImportOperatorProxy>(deserialized_filter_proxy->LeftInput());
  ASSERT_TRUE(deserialized_import_proxy);
  ASSERT_EQ(deserialized_import_proxy->InputNodeCount(), 0);

  EXPECT_EQ(serialized_proxy, SerializePqp(deserialized_proxy));
}

TEST_F(PqpSerializationTest, OperatorTree) {
  const auto import_proxy = ImportOperatorProxy::Make(kImportObjects, kImportColumnIds);
  const auto filter_proxy_1 =
      FilterOperatorProxy::Make(LessThanEquals_(PqpColumn_(ColumnId{1}, DataType::kLong, false, "a"), 0), import_proxy);
  const auto filter_proxy_2 = FilterOperatorProxy::Make(
      GreaterThanEquals_(PqpColumn_(ColumnId{0}, DataType::kLong, false, "a"), 100), import_proxy);
  const auto union_all_proxy = UnionOperatorProxy::Make(SetOperationMode::kAll, filter_proxy_1, filter_proxy_2);

  std::string serialized_proxy = SerializePqp(union_all_proxy);
  const auto deserialized_proxy = DeserializePqp(serialized_proxy);

  const auto deserialized_union_all_proxy = std::dynamic_pointer_cast<UnionOperatorProxy>(deserialized_proxy);
  ASSERT_TRUE(deserialized_union_all_proxy);
  ASSERT_EQ(deserialized_union_all_proxy->InputNodeCount(), 2);

  const auto deserialized_filter_proxy_1 = deserialized_union_all_proxy->LeftInput();
  ASSERT_TRUE(deserialized_filter_proxy_1);
  ASSERT_EQ(deserialized_filter_proxy_1->InputNodeCount(), 1);

  const auto deserialized_filter_proxy_2 = deserialized_union_all_proxy->RightInput();
  ASSERT_TRUE(deserialized_filter_proxy_2);
  ASSERT_EQ(deserialized_filter_proxy_2->InputNodeCount(), 1);

  EXPECT_NE(deserialized_filter_proxy_1, deserialized_filter_proxy_2);
  EXPECT_EQ(deserialized_filter_proxy_1->LeftInput(), deserialized_filter_proxy_2->LeftInput());

  const auto deserialized_import_proxy =
      std::dynamic_pointer_cast<ImportOperatorProxy>(deserialized_filter_proxy_1->LeftInput());
  ASSERT_TRUE(deserialized_import_proxy);
  EXPECT_EQ(deserialized_import_proxy->InputNodeCount(), 0);

  EXPECT_EQ(serialized_proxy, SerializePqp(deserialized_union_all_proxy));
}

TEST_F(PqpSerializationTest, CyclicGraph) {
  // In practise, we always have directed acyclic graphs. Therefore, this example is theoretical and solely for testing.
  const auto import_proxy = ImportOperatorProxy::Make(kImportObjects, kImportColumnIds);
  const auto filter_proxy = FilterOperatorProxy::Make(
      GreaterThanEquals_(PqpColumn_(ColumnId{0}, DataType::kLong, false, "a"), 100), import_proxy);
  const auto export_proxy = ExportOperatorProxy::Make(kTargetObject, ExportFormat::kOrc, filter_proxy);
  import_proxy->SetLeftInput(export_proxy);

  const std::string serialized_proxy = SerializePqp(export_proxy);
  const auto deserialized_proxy = DeserializePqp(serialized_proxy);

  EXPECT_EQ(deserialized_proxy->LeftInput()->LeftInput()->LeftInput(), deserialized_proxy);
}

}  // namespace skyrise
