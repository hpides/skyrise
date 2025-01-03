#include "metadata/schema/glue_catalog.hpp"

#include <gtest/gtest.h>

#include "client/coordinator_client.hpp"
#include "storage/backend/s3_storage.hpp"
#include "testing/aws_test.hpp"

namespace skyrise {

class AwsGlueCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<CoordinatorClient>();
    TableColumnDefinitions columns;
    columns.emplace_back("a", DataType::kDouble, true);
    columns.emplace_back("b", DataType::kFloat, false);
    columns.emplace_back("c", DataType::kInt, false);
    columns.emplace_back("d", DataType::kLong, false);
    columns.emplace_back("f", DataType::kString, false);
    schema_ = std::make_shared<TableSchema>(columns);
    schema_->AddKeyConstraint(TableKeyConstraint(std::unordered_set<ColumnId>{1, 3}, KeyConstraintType::kPrimaryKey));
    schema_->AddKeyConstraint(TableKeyConstraint(std::unordered_set<ColumnId>{2}, KeyConstraintType::kUnique));

    database_name_ = "skyrise-test-database-" + RandomString(8, kCharacterSetLower);
    catalog_ = std::make_shared<GlueCatalog>(client_, database_name_);
    EXPECT_EQ(catalog_->GetDatabaseName(), database_name_);

    s3_storage_ = std::make_shared<S3Storage>(client_->GetS3Client(), kSkyriseTestBucket);
  }

  ObjectReference AddRandomDataDirectory(size_t num_files) {
    for (size_t i = 0; i < num_files; ++i) {
      const auto writer = s3_storage_->OpenForWriting(database_name_ + "/part" + std::to_string(i));
      writer->Close();
    }
    return ObjectReference(kSkyriseTestBucket, database_name_);
  }

  void TearDown() override {
    s3_storage_->Delete(database_name_);
    catalog_->DeleteDatabase();
  }

  static void CompareSchemas(const std::shared_ptr<const TableSchema>& a, const std::shared_ptr<const TableSchema>& b) {
    EXPECT_EQ(a->TableColumnCount(), b->TableColumnCount());
    for (size_t i = 0; i < a->TableColumnCount(); ++i) {
      EXPECT_EQ(a->GetTableColumnDefinition(i), b->GetTableColumnDefinition(i));
    }

    EXPECT_EQ(a->KeyConstraints().size(), b->KeyConstraints().size());
    for (size_t i = 0; i < a->KeyConstraints().size(); ++i) {
      EXPECT_EQ(a->KeyConstraints()[i], b->KeyConstraints()[i]);
    }
  };

  AwsApi aws_api_;
  std::string database_name_;
  std::shared_ptr<GlueCatalog> catalog_;
  std::shared_ptr<TableSchema> schema_;
  std::shared_ptr<CoordinatorClient> client_;

  std::shared_ptr<S3Storage> s3_storage_;
};

TEST_F(AwsGlueCatalogTest, CreateDeleteTableSchema) {
  const std::string table_name = RandomString(8);
  EXPECT_FALSE(catalog_->TableExists(table_name));
  catalog_->AddTableMetadata(table_name, schema_, ObjectReference("bucket", "directory"));
  EXPECT_TRUE(catalog_->TableExists(table_name));

  catalog_->DeleteTableMetadata(table_name);
  EXPECT_FALSE(catalog_->TableExists(table_name));
}

TEST_F(AwsGlueCatalogTest, GetTableSchema) {
  const std::string table_name = RandomString(8);
  catalog_->AddTableMetadata(table_name, schema_, ObjectReference("bucket", "directory"));
  const auto table_schema_a = catalog_->GetTableSchema(table_name);
  EXPECT_NE(table_schema_a, nullptr);
  CompareSchemas(table_schema_a, schema_);

  const auto catalog_b = std::make_shared<GlueCatalog>(client_, database_name_);
  const auto table_schema_b = catalog_->GetTableSchema(table_name);
  CompareSchemas(table_schema_a, table_schema_b);
}

TEST_F(AwsGlueCatalogTest, IgnoreCaseSensitivity) {
  const ObjectReference data_location("bucket", "directory");
  const std::string table_name = "Table_Name";
  const std::string table_name_lc = "table_name";

  catalog_->AddTableMetadata(table_name, schema_, data_location);
  EXPECT_ANY_THROW(catalog_->AddTableMetadata(table_name_lc, schema_, data_location));

  const auto schema_a = catalog_->GetTableSchema(table_name);
  const auto schema_b = catalog_->GetTableSchema(table_name_lc);
  EXPECT_EQ(schema_a, schema_b);
}

TEST_F(AwsGlueCatalogTest, TableSchemaPartitions) {
  const std::string table_name = RandomString(8);
  const size_t num_files = 128;
  const ObjectReference data_location = AddRandomDataDirectory(num_files);

  catalog_->AddTableMetadata(table_name, schema_, data_location);
  EXPECT_EQ(catalog_->GetTablePartitions(table_name).size(), num_files);
}

TEST_F(AwsGlueCatalogTest, ListTableSchemas) {
  const ObjectReference data_location("bucket", "directory");
  const size_t num_tables = 12;
  std::vector<std::string> expected_tables_names;
  expected_tables_names.reserve(num_tables);
  for (size_t i = 0; i < num_tables; ++i) {
    expected_tables_names.push_back(RandomString(8, kCharacterSetLower));
    catalog_->AddTableMetadata(expected_tables_names.back(), schema_, data_location);
  }

  const std::vector<std::string> tables = catalog_->Tables();
  const std::set<std::string> table_names(tables.begin(), tables.end());
  EXPECT_EQ(tables.size(), expected_tables_names.size());
  for (const auto& expected_table_name : expected_tables_names) {
    EXPECT_TRUE(table_names.find(expected_table_name) != table_names.end());
  }
}

TEST_F(AwsGlueCatalogTest, AddAndGetManifest) {
  const std::string table_name = RandomString(8);
  catalog_->AddTableMetadata(table_name, schema_, ObjectReference("bucket", "directory"));

  const ObjectReference expected_manifest("b", "i", "e");
  catalog_->AddTableManifest(table_name, expected_manifest);

  const auto read_manifest = catalog_->TableManifest(table_name);
  EXPECT_EQ(*read_manifest, expected_manifest);

  // As adding the manifest is done by a table update, we need to ensure that the update operations has not removed any
  // other values. We double-check the schema here to ensure that the table parameters
  // (e.g., table key constraints) and the storage descriptor (e.g., column definition) still holds the old data.
  const auto schema = catalog_->GetTableSchema(table_name);
  CompareSchemas(schema, schema_);
}

}  // namespace skyrise
