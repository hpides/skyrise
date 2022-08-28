#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "types.hpp"

namespace skyrise {

class ImportOperatorProxyTest : public ::testing::Test {
 public:
  void SetUp() override {
    column_definitions_a_ = std::make_shared<TableColumnDefinitions>();
    column_definitions_a_->emplace_back("a", DataType::kInt, false);

    OrcFormatReaderOptions orc_options;
    orc_options.expected_schema = column_definitions_a_;
    orc_options.select_partition_range = std::make_pair(1, 1);
    import_options_orc_ = std::make_shared<const ImportOptions>(orc_options);

    CsvFormatReaderOptions csv_options;
    csv_options.expected_schema = column_definitions_a_;
    import_options_csv_ = std::make_shared<const ImportOptions>(csv_options);
  }

 protected:
  static inline const std::vector<ObjectReference> kObjectReferences = {
      ObjectReference{"dummy_bucket", "key1.orc", "etag1"}, ObjectReference{"dummy_bucket", "key2.orc", "etag1"},
      ObjectReference{"dummy_bucket", "key3.orc", "etag3"}};
  static inline const std::vector<ColumnId> kColumnIds = {ColumnId{0}, ColumnId{1}, ColumnId{3}};
  std::shared_ptr<TableColumnDefinitions> column_definitions_a_;
  std::shared_ptr<const ImportOptions> import_options_orc_;
  std::shared_ptr<const ImportOptions> import_options_csv_;
};

TEST_F(ImportOperatorProxyTest, BaseProperties) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  EXPECT_EQ(import_proxy->Type(), OperatorType::kImport);
  EXPECT_EQ(import_proxy->OriginIdentifier(), "");
  EXPECT_EQ(import_proxy->ObjectReferences(), kObjectReferences);
  EXPECT_EQ(import_proxy->ColumnIds(), kColumnIds);
  EXPECT_FALSE(import_proxy->IsPipelineBreaker());
}

TEST_F(ImportOperatorProxyTest, Description) {
  std::vector<ColumnId> column_ids = {0};
  const std::vector<ObjectReference> object_reference = {ObjectReference{"dummy_bucket", "dummy_object"}};
  const auto import_proxy = ImportOperatorProxy::Make(object_reference, column_ids);

  EXPECT_EQ(import_proxy->Description(DescriptionMode::kSingleLine), "[Import] dummy_bucket/dummy_object ColumnIds{0}");
  EXPECT_EQ(import_proxy->Description(DescriptionMode::kMultiLine),
            "[Import]\ndummy_bucket/\ndummy_object\nColumnIds{0}");

  const auto import_proxy_with_origin = ImportOperatorProxy::Make(object_reference, column_ids, "lineitem");
  EXPECT_EQ(import_proxy_with_origin->Description(DescriptionMode::kSingleLine),
            "[Import] dummy_bucket/dummy_object ColumnIds{0} – Origin: lineitem");
  EXPECT_EQ(import_proxy_with_origin->Description(DescriptionMode::kMultiLine),
            "[Import]\ndummy_bucket/\ndummy_object\nColumnIds{0}\nOrigin: lineitem");
}

TEST_F(ImportOperatorProxyTest, DescriptionMultipleObjects) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);

  EXPECT_EQ(import_proxy->Description(DescriptionMode::kSingleLine),
            "[Import] dummy_bucket/{3 objects} ColumnIds{0, 1, 3}");
  EXPECT_EQ(import_proxy->Description(DescriptionMode::kMultiLine),
            "[Import]\ndummy_bucket/\n{3 objects}\nColumnIds{0, 1, 3}");
}

TEST_F(ImportOperatorProxyTest, SetImportOptions) {
  const auto proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  ASSERT_EQ(proxy->GetImportOptions(), nullptr);
  proxy->SetImportOptions(import_options_orc_);
  EXPECT_EQ(proxy->GetImportOptions(), import_options_orc_);

  const auto import_operator = proxy->GetOrCreateOperatorInstance();
  // After creating (and caching) an operator instance, it should no longer be possible to modify proxy attributes.
  EXPECT_THROW(proxy->SetImportOptions(import_options_csv_), std::logic_error);
}

TEST_F(ImportOperatorProxyTest, OriginAndDataTraits) {
  auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  EXPECT_TRUE(import_proxy->OriginIdentifier().empty());
  EXPECT_EQ(import_proxy->OutputDataTraits().column_count, 3);
  EXPECT_EQ(import_proxy->OutputDataTraits().object_count, 3);
  EXPECT_EQ(import_proxy->OutputDataTraits().partition_count, 1);

  // Change ObjectReferences
  std::vector<ObjectReference> object_references;
  object_references.emplace_back("bucket_a", "a_1.orc");
  object_references.emplace_back("bucket_a", "a_2.orc");
  object_references.emplace_back("bucket_b", "b_1.orc");
  object_references.emplace_back("bucket_b", "b_2.orc");
  import_proxy->SetObjectReferences(object_references);
  EXPECT_EQ(import_proxy->OutputDataTraits().object_count, 4);

  // Set Origin and DataTraits
  const std::string origin = "query_XYZ_pipeline_001";
  const size_t partition_count = 3;
  const size_t target_object_count = 2;
  import_proxy->SetOriginAndDataTraits(origin, partition_count, target_object_count);
  EXPECT_EQ(import_proxy->OriginIdentifier(), origin);
  EXPECT_EQ(import_proxy->OutputDataTraits().partition_count, partition_count);
  EXPECT_EQ(import_proxy->OutputDataTraits().object_count, target_object_count);

  // Try illegal parameters
  EXPECT_THROW(import_proxy->SetOriginAndDataTraits("", partition_count, target_object_count), std::logic_error);
  EXPECT_THROW(import_proxy->SetOriginAndDataTraits(origin, 0, target_object_count), std::logic_error);
  EXPECT_THROW(import_proxy->SetOriginAndDataTraits(origin, partition_count, 0), std::logic_error);
  EXPECT_THROW(import_proxy->SetOriginAndDataTraits(origin, partition_count, object_references.size() + 1), std::logic_error);
}

TEST_F(ImportOperatorProxyTest, SerializeAndDeserialize) {
  const auto proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  ASSERT_EQ(proxy->GetImportOptions(), nullptr);
  // (1) Serialize
  const auto proxy_json = proxy->ToJson();

  // (2) Deserialize & verify attributes
  const auto deserialized_proxy = ImportOperatorProxy::FromJson(proxy_json);
  const auto deserialized_import_proxy = std::dynamic_pointer_cast<ImportOperatorProxy>(deserialized_proxy);
  EXPECT_EQ(deserialized_import_proxy->ObjectReferences(), kObjectReferences);
  EXPECT_EQ(deserialized_import_proxy->ColumnIds(), kColumnIds);
  EXPECT_EQ(deserialized_import_proxy->GetImportOptions(), nullptr);

  // (3) Serialize again
  EXPECT_EQ(proxy_json, deserialized_proxy->ToJson());
}

TEST_F(ImportOperatorProxyTest, SerializeAndDeserializeImportOptionsOrc) {
  auto import_proxy_orc = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy_orc->SetImportOptions(import_options_orc_);
  // (1) Serialize
  const auto proxy_orc_json = import_proxy_orc->ToJson();

  // (2) Deserialize
  const auto deserialized_proxy_orc = ImportOperatorProxy::FromJson(proxy_orc_json);
  ASSERT_NE(std::static_pointer_cast<ImportOperatorProxy>(deserialized_proxy_orc)->GetImportOptions(), nullptr);

  // (3) Serialize again
  const auto deserialized_proxy_orc_json = deserialized_proxy_orc->ToJson();
  EXPECT_EQ(deserialized_proxy_orc_json, proxy_orc_json);
}

TEST_F(ImportOperatorProxyTest, SerializeAndDeserializeImportOptionsCsv) {
  const auto import_proxy_csv = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy_csv->SetImportOptions(import_options_csv_);
  // (1) Serialize
  const auto proxy_csv_json = import_proxy_csv->ToJson();

  // (2) Deserialize
  const auto deserialized_proxy_csv = ImportOperatorProxy::FromJson(proxy_csv_json);
  ASSERT_NE(std::static_pointer_cast<ImportOperatorProxy>(deserialized_proxy_csv)->GetImportOptions(), nullptr);

  // (3) Serialize again
  const auto deserialized_proxy_csv_json = deserialized_proxy_csv->ToJson();
  EXPECT_EQ(deserialized_proxy_csv_json, proxy_csv_json);
}

TEST_F(ImportOperatorProxyTest, DeepCopy) {
  auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy->SetImportOptions(import_options_csv_);

  const std::string origin = "query_XYZ_pipeline_001";
  const size_t partition_count = 3;
  const size_t target_object_count = 2;
  import_proxy->SetOriginAndDataTraits(origin, partition_count, target_object_count);

  // Verify DeepCopy
  const auto import_proxy_copy = std::dynamic_pointer_cast<ImportOperatorProxy>(import_proxy->DeepCopy());
  EXPECT_EQ(import_proxy_copy->ObjectReferences(), kObjectReferences);
  EXPECT_EQ(import_proxy_copy->ColumnIds(), kColumnIds);
  EXPECT_EQ(import_proxy_copy->GetImportOptions(), import_options_csv_);

  EXPECT_EQ(import_proxy_copy->OriginIdentifier(), origin);
  EXPECT_EQ(import_proxy_copy->OutputDataTraits().column_count, kColumnIds.size());
  EXPECT_EQ(import_proxy_copy->OutputDataTraits().object_count, target_object_count);
  EXPECT_EQ(import_proxy_copy->OutputDataTraits().partition_count, partition_count);
}

TEST_F(ImportOperatorProxyTest, CreateOperatorInstance) {
  // When custom reader options for CSV/ORC are not provided, default options must be derived. In the latter case,
  // the provided object keys must specify an .orc or .csv file extension.
  {
    const std ::vector<ObjectReference> object_references = {ObjectReference("dummy_bucket", "key1"),
                                                             ObjectReference("dummy_bucket", "key2")};
    const auto import_proxy = ImportOperatorProxy::Make(object_references, kColumnIds);
    EXPECT_THROW(import_proxy->GetOrCreateOperatorInstance(), std::logic_error);
  }
  {
    const std ::vector<ObjectReference> object_references = {ObjectReference("dummy_bucket", "key1.csv"),
                                                             ObjectReference("dummy_bucket", "key2.csv")};
    const auto import_proxy = ImportOperatorProxy::Make(object_references, kColumnIds);
    EXPECT_TRUE(import_proxy->GetOrCreateOperatorInstance());
    EXPECT_EQ(import_proxy->GetOrCreateOperatorInstance()->Type(), OperatorType::kImport);
  }
  {
    const std ::vector<ObjectReference> object_references = {ObjectReference("dummy_bucket", "key1.orc"),
                                                             ObjectReference("dummy_bucket", "key2.orc")};
    const auto import_proxy = ImportOperatorProxy::Make(object_references, kColumnIds);
    EXPECT_TRUE(import_proxy->GetOrCreateOperatorInstance());
    EXPECT_EQ(import_proxy->GetOrCreateOperatorInstance()->Type(), OperatorType::kImport);
  }
}

TEST_F(ImportOperatorProxyTest, CreateOperatorInstanceCustomCsvOptions) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy->SetImportOptions(import_options_csv_);
  EXPECT_TRUE(import_proxy->GetOrCreateOperatorInstance());
}

TEST_F(ImportOperatorProxyTest, CreateOperatorInstanceCustomOrcOptions) {
  const auto import_proxy = ImportOperatorProxy::Make(kObjectReferences, kColumnIds);
  import_proxy->SetImportOptions(import_options_orc_);
  EXPECT_TRUE(import_proxy->GetOrCreateOperatorInstance());
}

}  // namespace skyrise
