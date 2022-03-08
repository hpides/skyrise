#include "compiler/physical_query_plan/export_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class ExportOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {}

 protected:
  static inline const std::string bucket_name_ = "dummy_bucket";
  static inline const std::string target_object_key_ = "dummy_target_object_key";
  static inline const auto export_format_ = ExportFormat::kCsv;
};

TEST_F(ExportOperatorProxyTest, BaseProperties) {
  auto export_proxy = ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_);
  EXPECT_EQ(export_proxy->Type(), OperatorType::kExport);
  EXPECT_EQ(export_proxy->BucketName(), bucket_name_);
  EXPECT_EQ(export_proxy->TargetObjectKey(), target_object_key_);
  EXPECT_EQ(export_proxy->GetExportFormat(), export_format_);
  EXPECT_FALSE(export_proxy->IsPipelineBreaker());
}

TEST_F(ExportOperatorProxyTest, Description) {
  auto export_proxy = ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_);

  EXPECT_EQ(export_proxy->Description(DescriptionMode::kSingleLine), "[Export] dummy_bucket/dummy_target_object_key");
  EXPECT_EQ(export_proxy->Description(DescriptionMode::kMultiLine), "[Export]\ndummy_bucket/\ndummy_target_object_key");
}

TEST_F(ExportOperatorProxyTest, SerializeAndDeserialize) {
  auto proxy = ExportOperatorProxy::Make(bucket_name_, target_object_key_, export_format_);
  // (1) Serialize
  auto export_json = proxy->ToJson();

  // (2) Deserialize & verify attributes
  auto deserialized_proxy = ExportOperatorProxy::FromJson(export_json);
  auto deserialized_export_proxy = std::dynamic_pointer_cast<ExportOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(deserialized_export_proxy->BucketName(), bucket_name_);
  EXPECT_EQ(deserialized_export_proxy->TargetObjectKey(), target_object_key_);
  EXPECT_EQ(deserialized_export_proxy->GetExportFormat(), export_format_);

  // (3) Serialize again
  auto deserialized_proxy_json = deserialized_proxy->ToJson();
  EXPECT_EQ(export_json, deserialized_proxy_json);
}

TEST_F(ExportOperatorProxyTest, DummyExportOperatorProxy) {
  std::shared_ptr<AbstractOperatorProxy> proxy = ExportOperatorProxy::DummyExportOperatorProxy();
  auto export_proxy = std::dynamic_pointer_cast<ExportOperatorProxy>(proxy);
  EXPECT_EQ(export_proxy->BucketName(), "PLACEHOLDER");
  EXPECT_EQ(export_proxy->TargetObjectKey(), "PLACEHOLDER");
  EXPECT_EQ(export_proxy->GetExportFormat(), ExportFormat::kOrc);
}

TEST_F(ExportOperatorProxyTest, DeepCopy) {
  // clang-format off
  auto export_proxy =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, ExportFormat::kCsv,
    ImportOperatorProxy::Make(bucket_name_, std::vector<std::string>{"import.orc"}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  auto export_proxy_copy = std::dynamic_pointer_cast<ExportOperatorProxy>(export_proxy->DeepCopy());
  EXPECT_EQ(export_proxy_copy->GetExportFormat(), ExportFormat::kCsv);
  EXPECT_EQ(export_proxy_copy->BucketName(), bucket_name_);
  EXPECT_EQ(export_proxy_copy->TargetObjectKey(), target_object_key_);
  EXPECT_EQ(export_proxy_copy->InputNodeCount(), 1);
  // Without input
  export_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(export_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(ExportOperatorProxyTest, CreateOperatorInstance) {
  // clang-format off
  auto export_proxy_orc =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, ExportFormat::kOrc,
    ImportOperatorProxy::Make(bucket_name_, std::vector<std::string>{"import.orc"}, std::vector<ColumnId>{ColumnId{0}}));

  auto export_proxy_csv =
  ExportOperatorProxy::Make(bucket_name_, target_object_key_, ExportFormat::kCsv,
    ImportOperatorProxy::Make(bucket_name_, std::vector<std::string>{"import.orc"}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  EXPECT_NE(export_proxy_orc->GetOrCreateOperatorInstance(), nullptr);
  EXPECT_EQ(export_proxy_orc->GetOrCreateOperatorInstance()->Type(), OperatorType::kExport);
  EXPECT_NE(export_proxy_csv->GetOrCreateOperatorInstance(), nullptr);
  EXPECT_EQ(export_proxy_csv->GetOrCreateOperatorInstance()->Type(), OperatorType::kExport);
}

}  // namespace skyrise
