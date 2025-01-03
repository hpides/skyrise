#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "types.hpp"

namespace skyrise {

class ExportOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {}

 protected:
  static inline const ObjectReference kTargetObject{"export_bucket", "export.csv"};
  static inline const ObjectReference kImportObject{"import_bucket", "import.csv"};
  static inline const auto kExportFormat = FileFormat::kCsv;
};

TEST_F(ExportOperatorProxyTest, BaseProperties) {
  const auto export_proxy = ExportOperatorProxy::Make(kTargetObject, kExportFormat);
  EXPECT_EQ(export_proxy->Type(), OperatorType::kExport);
  EXPECT_EQ(export_proxy->TargetObject(), kTargetObject);
  EXPECT_EQ(export_proxy->GetExportFormat(), kExportFormat);
  EXPECT_FALSE(export_proxy->IsPipelineBreaker());
}

TEST_F(ExportOperatorProxyTest, Description) {
  const auto export_proxy = ExportOperatorProxy::Make(kTargetObject, kExportFormat);

  EXPECT_EQ(export_proxy->Description(DescriptionMode::kSingleLine), "[Export] export_bucket/export.csv");
  EXPECT_EQ(export_proxy->Description(DescriptionMode::kMultiLine), "[Export]\nexport_bucket/\nexport.csv");
}

TEST_F(ExportOperatorProxyTest, SerializeAndDeserialize) {
  const auto proxy = ExportOperatorProxy::Make(kTargetObject, kExportFormat);
  // (1) Serialize
  const auto export_json = proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = ExportOperatorProxy::FromJson(export_json);
  const auto deserialized_export_proxy = std::dynamic_pointer_cast<ExportOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(deserialized_export_proxy->TargetObject(), kTargetObject);
  EXPECT_EQ(deserialized_export_proxy->GetExportFormat(), kExportFormat);

  // (3) Serialize again
  const auto deserialized_proxy_json = deserialized_proxy->ToJson();
  EXPECT_EQ(export_json, deserialized_proxy_json);
}

TEST_F(ExportOperatorProxyTest, Dummy) {
  const auto export_proxy = std::static_pointer_cast<ExportOperatorProxy>(ExportOperatorProxy::Dummy());
  EXPECT_EQ(export_proxy->TargetObject().bucket_name, "Placeholder");
  EXPECT_EQ(export_proxy->TargetObject().identifier, "Placeholder");
  EXPECT_EQ(export_proxy->GetExportFormat(), FileFormat::kCsv);

  // Optionally, an input proxy can be set
  // clang-format off
  const auto pqp =
  ExportOperatorProxy::Dummy(
    ImportOperatorProxy::Make(std::vector<ObjectReference>{kImportObject}, std::vector<ColumnId>{ColumnId{0}}));
  // clang-format on
  EXPECT_TRUE(pqp->LeftInput() && pqp->LeftInput()->Type() == OperatorType::kImport);
}

TEST_F(ExportOperatorProxyTest, DeepCopy) {
  // clang-format off
  const auto export_proxy =
  ExportOperatorProxy::Make(kTargetObject, FileFormat::kCsv,
                            ImportOperatorProxy::Make(std::vector<ObjectReference>{kImportObject}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  const auto export_proxy_copy = std::dynamic_pointer_cast<ExportOperatorProxy>(export_proxy->DeepCopy());
  EXPECT_EQ(export_proxy_copy->GetExportFormat(), FileFormat::kCsv);
  EXPECT_EQ(export_proxy_copy->TargetObject(), kTargetObject);
  EXPECT_EQ(export_proxy_copy->InputNodeCount(), 1);
  // Without input
  export_proxy->SetLeftInput(nullptr);
  EXPECT_EQ(export_proxy->DeepCopy()->InputNodeCount(), 0);
}

TEST_F(ExportOperatorProxyTest, CreateOperatorInstance) {
  // clang-format off
  const auto export_proxy_orc =
  ExportOperatorProxy::Make(kTargetObject, FileFormat::kOrc,
                            ImportOperatorProxy::Make(std::vector<ObjectReference>{kImportObject}, std::vector<ColumnId>{ColumnId{0}}));

  const auto export_proxy_csv =
  ExportOperatorProxy::Make(kTargetObject, FileFormat::kCsv,
                            ImportOperatorProxy::Make(std::vector<ObjectReference>{kImportObject}, std::vector<ColumnId>{ColumnId{0}}));

  // clang-format on
  EXPECT_NE(export_proxy_orc->GetOrCreateOperatorInstance(), nullptr);
  EXPECT_EQ(export_proxy_orc->GetOrCreateOperatorInstance()->Type(), OperatorType::kExport);
  EXPECT_NE(export_proxy_csv->GetOrCreateOperatorInstance(), nullptr);
  EXPECT_EQ(export_proxy_csv->GetOrCreateOperatorInstance()->Type(), OperatorType::kExport);
}

}  // namespace skyrise
