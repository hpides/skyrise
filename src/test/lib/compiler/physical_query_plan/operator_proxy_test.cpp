#include <gtest/gtest.h>

#include "compiler/physical_query_plan/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/partition_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_serialization_constants.hpp"
#include "operator/export_operator.hpp"
#include "operator/import_operator.hpp"
#include "operator/partition_operator.hpp"
#include "storage/backend/mock_storage.hpp"
#include "types.hpp"

namespace skyrise {

class ProxyOperatorTest : public ::testing::Test {
  void SetUp() override {
    mock_storage_factory_ = [](const std::string& /*storage_identifier*/) -> std::shared_ptr<Storage> {
      return std::make_shared<MockStorage>();
    };
  }

 protected:
  StorageFactory mock_storage_factory_;
};

template <typename Proxy, typename Operator>
void TestProxy(std::shared_ptr<const Proxy> proxy) {
  std::shared_ptr<AbstractOperator> operator_instance = proxy->GetOperatorInstance();
  ASSERT_NE(operator_instance, nullptr);
  std::shared_ptr<const Operator> deserialized_proxy = std::dynamic_pointer_cast<const Operator>(operator_instance);
  ASSERT_NE(operator_instance, nullptr);

  std::optional<std::string> name = proxy->Name();
  EXPECT_TRUE(name.has_value());

  Aws::Utils::Json::JsonValue proxy_json1 = proxy->ToJson();
  Aws::Utils::Json::JsonValue proxy_json2 = Proxy::FromJson(proxy_json1)->ToJson();
  proxy_json1.WithString(kKeyLeftInput, "");
  proxy_json1.WithString(kKeyRightInput, "");
  proxy_json2.WithString(kKeyLeftInput, "");
  proxy_json2.WithString(kKeyRightInput, "");
  ASSERT_EQ(proxy_json1, proxy_json2);
}

TEST_F(ProxyOperatorTest, AbstractProxyTest) {
  // We cannot create an instance of AbstractOperatorProxy, thus we use the ExportOperatorProxy to check the correct
  // serialization.
  auto left_child = std::make_shared<const ExportOperatorProxy>("", "", ExportOperator::OutputFormat::kOrc);
  auto right_child = std::make_shared<const ExportOperatorProxy>("", "", ExportOperator::OutputFormat::kOrc);
  auto proxy =
      std::make_shared<const ExportOperatorProxy>("", "", ExportOperator::OutputFormat::kOrc, left_child, right_child);

  const Aws::Utils::Json::JsonValue proxy_json = proxy->ToJson();

  ASSERT_FALSE(proxy_json.View().GetString(kKeyOperatorType).empty());
  ASSERT_EQ(proxy_json.View().GetString(kKeyLeftInput), left_child->GetIdentity());
  ASSERT_EQ(proxy_json.View().GetString(kKeyRightInput), right_child->GetIdentity());
}

TEST_F(ProxyOperatorTest, ImportOperatorProxyTest) {
  std::string bucket_name = "test_bucket";
  std::vector<std::string> object_keys = {"a", "b", "c"};
  std::vector<ColumnId> column_ids = {ColumnId{2}, ColumnId{3}};

  auto import_orc_proxy = std::make_shared<ImportOperatorProxy>(bucket_name, object_keys, column_ids,
                                                                ImportOperatorProxy::ObjectFormat::kOrc);
  auto import_csv_proxy = std::make_shared<ImportOperatorProxy>(bucket_name, object_keys, column_ids,
                                                                ImportOperatorProxy::ObjectFormat::kCsv);

  import_csv_proxy->SetStorageFactory(mock_storage_factory_);
  import_orc_proxy->SetStorageFactory(mock_storage_factory_);

  TestProxy<ImportOperatorProxy, ImportOperator>(import_orc_proxy);
  TestProxy<ImportOperatorProxy, ImportOperator>(import_csv_proxy);
}

TEST_F(ProxyOperatorTest, ExportOperatorProxyTest) {
  std::string bucket_name = "test_bucket";
  std::string target_file = "target_file_name";
  auto format = ExportOperator::OutputFormat::kOrc;

  auto export_proxy = std::make_shared<ExportOperatorProxy>(bucket_name, target_file, format);

  // We do not need real storage for this test.
  export_proxy->SetStorageFactory([](const std::string& /*unused*/) { return nullptr; });

  TestProxy<ExportOperatorProxy, ExportOperator>(export_proxy);
}

TEST_F(ProxyOperatorTest, PartitionOperatorProxyTest) {
  const size_t partition_count = 10;
  const std::set<ColumnId> partition_column_ids{0, 1};

  auto partition_proxy = std::make_shared<const PartitionOperatorProxy>(partition_count, partition_column_ids);

  TestProxy<PartitionOperatorProxy, PartitionOperator>(partition_proxy);
}

}  // namespace skyrise
