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
  static inline const std::string kBucketName = "dummy_bucket";
  static inline const std::string kTargetObjectKey = "dummy_target_object_key";
  static inline const auto kExportFormat = ExportFormat::kCsv;
};

TEST_F(ExportOperatorProxyTest, BaseProperties) {
  const auto export_proxy = ExportOperatorProxy::Make(kBucketName, kTargetObjectKey, kExportFormat);
  EXPECT_EQ(export_proxy->Type(), OperatorType::kExport);
  EXPECT_EQ(export_proxy->BucketName(), kBucketName);
  EXPECT_EQ(export_proxy->TargetObjectKey(), kTargetObjectKey);
  EXPECT_EQ(export_proxy->GetExportFormat(), kExportFormat);
  EXPECT_FALSE(export_proxy->IsPipelineBreaker());
}

TEST_F(ExportOperatorProxyTest, Description) {
  const auto export_proxy = ExportOperatorProxy::Make(kBucketName, kTargetObjectKey, kExportFormat);

  EXPECT_EQ(export_proxy->Description(DescriptionMode::kSingleLine), "[Export] dummy_bucket/dummy_target_object_key");
  EXPECT_EQ(export_proxy->Description(DescriptionMode::kMultiLine), "[Export]\ndummy_bucket/\ndummy_target_object_key");
}

TEST_F(ExportOperatorProxyTest, SerializeAndDeserialize) {
  const auto proxy = ExportOperatorProxy::Make(kBucketName, kTargetObjectKey, kExportFormat);
  // (1) Serialize
  const auto export_json = proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = ExportOperatorProxy::FromJson(export_json);
  const auto deserialized_export_proxy = std::dynamic_pointer_cast<ExportOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(deserialized_export_proxy->BucketName(), kBucketName);
  EXPECT_EQ(deserialized_export_proxy->TargetObjectKey(), kTargetObjectKey);
  EXPECT_EQ(deserialized_export_proxy->GetExportFormat(), kExportFormat);

  // (3) Serialize again
  const auto deserialized_proxy_json = deserialized_proxy->ToJson();
  EXPECT_EQ(export_json, deserialized_proxy_json);
}

TEST_F(ExportOperatorProxyTest, DummyExportOperatorProxy) {
  std::shared_ptr<AbstractOperatorProxy> proxy = ExportOperatorProxy::DummyExportOperatorProxy();
  const auto export_proxy = std::dynamic_pointer_cast<ExportOperatorProxy>(proxy);
  EXPECT_EQ(export_proxy->BucketName(), "PLACEHOLDER");
  EXPECT_EQ(export_proxy->TargetObjectKey(), "PLACEHOLDER");
  EXPECT_EQ(export_proxy->GetExportFormat(), ExportFormat::kOrc);
}

TEST_F(ExportOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto export_proxy =
  ExportOperatorProxy::Make(kBucketName, kTargetObjectKey, ExportFormat::kCsv,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference(kBucketName, "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  const auto export_proxy_copy = std::dynamic_pointer_cast<ExportOperatorProxy>(export_proxy->DeepCopy());
  EXPECT_EQ(export_proxy_copy->GetExportFormat(), ExportFormat::kCsv);
  EXPECT_EQ(export_proxy_copy->BucketName(), kBucketName);
  EXPECT_EQ(export_proxy_copy->TargetObjectKey(), kTargetObjectKey);
  EXPECT_EQ(export_proxy_copy->InputNodeCount(), 1);
  // Without input
  export_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(export_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(ExportOperatorProxyTest, CreateOperatorInstance) {
  // clang-format off
  const auto export_proxy_orc =
  ExportOperatorProxy::Make(kBucketName, kTargetObjectKey, ExportFormat::kOrc,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference(kBucketName, "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));

  const auto export_proxy_csv =
  ExportOperatorProxy::Make(kBucketName, kTargetObjectKey, ExportFormat::kCsv,
    ImportOperatorProxy::Make(std::vector<ObjectReference>{ObjectReference(kBucketName, "import.orc")}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  EXPECT_NE(export_proxy_orc->GetOrCreateOperatorInstance(), nullptr);
  EXPECT_EQ(export_proxy_orc->GetOrCreateOperatorInstance()->Type(), OperatorType::kExport);
  EXPECT_NE(export_proxy_csv->GetOrCreateOperatorInstance(), nullptr);
  EXPECT_EQ(export_proxy_csv->GetOrCreateOperatorInstance()->Type(), OperatorType::kExport);
}

}  // namespace skyrise
