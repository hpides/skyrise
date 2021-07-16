#include <gtest/gtest.h>

#include "compiler/physical_query_plan/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "compiler/physical_query_plan/partition_operator_proxy.hpp"
#include "compiler/physical_query_plan/pqp_serialization_constants.hpp"
#include "operator/partition_operator.hpp"
#include "types.hpp"

namespace skyrise {

template <typename Proxy, typename Operator>
void TestProxy(std::shared_ptr<const Proxy> proxy) {
  std::shared_ptr<AbstractOperator> operator_instance = proxy->GetOperatorInstance();
  // TODO(anyone): Uncomment when ImportOperator and ExportOperator are implemented.
  // ASSERT_NE(operator_instance, nullptr);
  // std::shared_ptr<const Operator> deserialized_import_proxy = std::dynamic_pointer_cast<const
  // Operator>(operator_instance); ASSERT_NE(operator_instance, nullptr);

  Aws::Utils::Json::JsonValue proxy_json1 = proxy->ToJson();
  Aws::Utils::Json::JsonValue proxy_json2 = Proxy::FromJson(proxy_json1)->ToJson();
  proxy_json1.WithString(kKeyLeftInput, "");
  proxy_json1.WithString(kKeyRightInput, "");
  proxy_json2.WithString(kKeyLeftInput, "");
  proxy_json2.WithString(kKeyRightInput, "");
  ASSERT_EQ(proxy_json1, proxy_json2);
}

TEST(ProxyOperatorTest, AbstractProxyTest) {
  // We cannot create an instance of AbstractOperatorProxy, thus we use the ExportOperatorProxy to check the correct
  // serialization.
  auto left_child = std::make_shared<const ExportOperatorProxy>("", "");
  auto right_child = std::make_shared<const ExportOperatorProxy>("", "");
  auto proxy = std::make_shared<const ExportOperatorProxy>("", "", left_child, right_child);

  const Aws::Utils::Json::JsonValue proxy_json = proxy->ToJson();

  ASSERT_FALSE(proxy_json.View().GetString(kKeyOperatorType).empty());
  ASSERT_EQ(proxy_json.View().GetString(kKeyLeftInput), left_child->GetIdentity());
  ASSERT_EQ(proxy_json.View().GetString(kKeyRightInput), right_child->GetIdentity());
}

TEST(ProxyOperatorTest, ImportOperatorProxyTest) {
  std::string bucket_name = "test_bucket";
  std::vector<std::string> object_keys = {"a", "b", "c"};
  std::vector<ColumnId> pruned_column_ids = {ColumnId{2}, ColumnId{3}};
  ImportOperatorProxy::ObjectFormat object_format = ImportOperatorProxy::ObjectFormat::kOrc;

  auto import_proxy =
      std::make_shared<const ImportOperatorProxy>(bucket_name, object_keys, pruned_column_ids, object_format);

  // TODO(anyone): Second type has to be ImportOperator.
  TestProxy<ImportOperatorProxy, ImportOperatorProxy>(import_proxy);
}

TEST(ProxyOperatorTest, ExportOperatorProxyTest) {
  std::string bucket_name = "test_bucket";
  std::string target_object_key = "target_object_key";

  auto export_proxy = std::make_shared<const ExportOperatorProxy>(bucket_name, target_object_key);

  // TODO(anyone): Second type has to be ExportOperator.
  TestProxy<ExportOperatorProxy, ExportOperatorProxy>(export_proxy);
}

TEST(ProxyOperatorTest, PartitionOperatorProxyTest) {
  const size_t partition_count = 10;
  const std::set<ColumnId> partition_column_ids{0, 1};

  auto partition_proxy = std::make_shared<const PartitionOperatorProxy>(partition_count, partition_column_ids);

  TestProxy<PartitionOperatorProxy, PartitionOperator>(partition_proxy);
}

}  // namespace skyrise
