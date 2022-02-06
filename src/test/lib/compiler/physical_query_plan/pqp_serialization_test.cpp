#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_deserializer.hpp"
#include "compiler/physical_query_plan/pqp_serializer.hpp"
#include "types.hpp"

namespace skyrise {

// TODO(anyone): When we have more operators with a stable interface, these tests should not "abuse" ImportOperatorProxy
// and ExportOperatorProxy to form artificial plans.
class PqpSerializerTest : public ::testing::Test {
 protected:
  std::string bucket_name_ = "test_bucket";
  std::vector<std::string> object_keys_{"a", "b", "c"};
  std::vector<ColumnId> column_ids_ = {ColumnId{2}, ColumnId{3}};
};

TEST_F(PqpSerializerTest, SingleOperatorProxySerializationTest) {
  auto import_proxy = std::make_shared<ImportOperatorProxy>(bucket_name_, object_keys_, column_ids_);

  auto serializer = PqpSerializer(import_proxy);
  std::string serialized_proxy = serializer.Serialize();

  auto deserializer = PqpDeserializer(serialized_proxy);
  std::shared_ptr<AbstractOperatorProxy> deserialized_proxy = deserializer.Deserialize();

  std::shared_ptr<ImportOperatorProxy> deserialized_import_proxy =
      std::dynamic_pointer_cast<ImportOperatorProxy>(deserialized_proxy);
  ASSERT_NE(deserialized_import_proxy, nullptr);

  // The ImportOperatorProxy does not contain left/right children. Thus, we do not need to care about identities and can
  // simply check the proxy jsons for equality.
  ASSERT_EQ(deserialized_import_proxy->ToJson(), import_proxy->ToJson());
}

TEST_F(PqpSerializerTest, LinearOperatorProxySerializationTest) {
  auto import_proxy = std::make_shared<ImportOperatorProxy>(bucket_name_, object_keys_, column_ids_);
  auto export_proxy =
      std::make_shared<ExportOperatorProxy>(bucket_name_, "", ExportOperator::OutputFormat::kOrc, import_proxy);

  auto serializer = PqpSerializer(export_proxy);
  std::string serialized_proxy = serializer.Serialize();

  auto deserializer = PqpDeserializer(serialized_proxy);
  auto deserialized_proxy = deserializer.Deserialize();

  std::shared_ptr<ExportOperatorProxy> deserialized_export_proxy =
      std::dynamic_pointer_cast<ExportOperatorProxy>(deserialized_proxy);
  ASSERT_NE(deserialized_export_proxy, nullptr);

  ASSERT_FALSE(deserialized_export_proxy->GetRightInput());
  ASSERT_TRUE(deserialized_export_proxy->GetLeftInput());

  std::shared_ptr<ImportOperatorProxy> deserialized_child_proxy =
      std::dynamic_pointer_cast<ImportOperatorProxy>(deserialized_export_proxy->GetLeftInput());
  ASSERT_NE(deserialized_child_proxy, nullptr);

  ASSERT_FALSE(deserialized_child_proxy->GetLeftInput());
  ASSERT_FALSE(deserialized_child_proxy->GetRightInput());
}

TEST_F(PqpSerializerTest, DagOperatorProxySerializationTest) {
  auto import_proxy = std::make_shared<ImportOperatorProxy>(bucket_name_, object_keys_, column_ids_);
  auto export_proxy1 =
      std::make_shared<ExportOperatorProxy>(bucket_name_, "", ExportOperator::OutputFormat::kOrc, import_proxy);
  auto export_proxy2 =
      std::make_shared<ExportOperatorProxy>(bucket_name_, "", ExportOperator::OutputFormat::kOrc, import_proxy);
  auto export_proxy_root = std::make_shared<ExportOperatorProxy>(bucket_name_, "", ExportOperator::OutputFormat::kOrc,
                                                                 export_proxy1, export_proxy2);

  auto serializer = PqpSerializer(export_proxy_root);
  std::string serialized_proxy = serializer.Serialize();

  auto deserializer = PqpDeserializer(serialized_proxy);
  std::shared_ptr<AbstractOperatorProxy> deserialized_proxy = deserializer.Deserialize();

  std::shared_ptr<ExportOperatorProxy> deserialized_export_proxy =
      std::dynamic_pointer_cast<ExportOperatorProxy>(deserialized_proxy);
  ASSERT_NE(deserialized_export_proxy, nullptr);

  ASSERT_NE(deserialized_export_proxy->GetLeftInput(), nullptr);
  ASSERT_NE(deserialized_export_proxy->GetRightInput(), nullptr);
  ASSERT_NE(deserialized_export_proxy->GetLeftInput(), deserialized_export_proxy->GetRightInput());

  std::shared_ptr<ExportOperatorProxy> deserialized_left_child_proxy =
      std::dynamic_pointer_cast<ExportOperatorProxy>(deserialized_export_proxy->GetLeftInput());
  ASSERT_NE(deserialized_left_child_proxy, nullptr);
  std::shared_ptr<ExportOperatorProxy> deserialized_right_child_proxy =
      std::dynamic_pointer_cast<ExportOperatorProxy>(deserialized_export_proxy->GetRightInput());
  ASSERT_NE(deserialized_right_child_proxy, nullptr);

  ASSERT_NE(deserialized_left_child_proxy->GetLeftInput(), nullptr);
  ASSERT_NE(deserialized_right_child_proxy->GetLeftInput(), nullptr);
  ASSERT_EQ(deserialized_left_child_proxy->GetRightInput(), nullptr);
  ASSERT_EQ(deserialized_right_child_proxy->GetRightInput(), nullptr);
  ASSERT_EQ(deserialized_left_child_proxy->GetLeftInput(), deserialized_right_child_proxy->GetLeftInput());

  std::shared_ptr<ImportOperatorProxy> deserialized_child_of_left_proxy =
      std::dynamic_pointer_cast<ImportOperatorProxy>(deserialized_left_child_proxy->GetLeftInput());
  ASSERT_NE(deserialized_child_of_left_proxy, nullptr);
  std::shared_ptr<ImportOperatorProxy> deserialized_child_of_right_proxy =
      std::dynamic_pointer_cast<ImportOperatorProxy>(deserialized_right_child_proxy->GetLeftInput());
  ASSERT_NE(deserialized_child_of_right_proxy, nullptr);
}

TEST_F(PqpSerializerTest, CircularOperatorProxySerializationTest) {
  auto proxy1 = std::make_shared<ExportOperatorProxy>(bucket_name_, "", ExportOperator::OutputFormat::kOrc);
  auto proxy2 = std::make_shared<ExportOperatorProxy>(bucket_name_, "", ExportOperator::OutputFormat::kOrc, proxy1);
  auto proxy_root = std::make_shared<ExportOperatorProxy>(bucket_name_, "", ExportOperator::OutputFormat::kOrc, proxy2);
  proxy1->SetLeftInput(proxy_root);

  auto serializer = PqpSerializer(proxy_root);
  std::string serialized_proxy = serializer.Serialize();

  auto deserializer = PqpDeserializer(serialized_proxy);
  std::shared_ptr<AbstractOperatorProxy> deserialized_proxy = deserializer.Deserialize();

  ASSERT_EQ(deserialized_proxy->GetLeftInput()->GetLeftInput()->GetLeftInput(), deserialized_proxy);
}
}  // namespace skyrise
