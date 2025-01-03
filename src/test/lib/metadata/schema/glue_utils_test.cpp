#include "metadata/schema/glue_utils.hpp"

#include <gtest/gtest.h>

#include "client/coordinator_client.hpp"
#include "testing/aws_test.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

class AwsGlueUtilsTest : public ::testing::Test {
 public:
  void SetUp() override {
    client_ = CoordinatorClient().GetGlueClient();
    database_name_ = "skyrise-test-database-" + RandomString(8, kCharacterSetLower);
    const auto outcome = CreateGlueDatabaseSchema(client_, database_name_);
    Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  }

  void TearDown() override {
    const auto outcome = DeleteGlueDatabaseSchema(client_, database_name_);
    Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  }

 protected:
  AwsApi aws_api_;
  std::shared_ptr<const Aws::Glue::GlueClient> client_;
  std::string database_name_;
};

TEST_F(AwsGlueUtilsTest, CreateReadDeleteTableSchema) {
  const auto table_name = RandomString(8, kCharacterSetLower);
  const std::string data_location = "data_location";
  std::map<std::string, std::string> parameters;
  parameters["A"] = "a";

  Aws::Glue::Model::StorageDescriptor storage;
  storage.SetLocation(data_location);

  const auto create_outcome = CreateGlueTableSchema(client_, database_name_, table_name, storage, parameters);
  Assert(create_outcome.IsSuccess(), create_outcome.GetError().GetMessage());
  EXPECT_TRUE(GlueTableSchemaExists(client_, database_name_, table_name).IsSuccess());

  const auto table = GlueTableSchema(client_, database_name_, table_name);
  EXPECT_EQ(table.GetDatabaseName(), database_name_);
  EXPECT_EQ(table.GetName(), table_name);
  EXPECT_EQ(table.GetVersionId(), "0");

  const auto& storage_descriptor = table.GetStorageDescriptor();
  EXPECT_EQ(storage_descriptor.GetLocation(), data_location);

  const auto& table_parameters = table.GetParameters();
  EXPECT_EQ(table_parameters, parameters);

  const auto delete_outcome = DeleteGlueTableSchema(client_, database_name_, table_name);
  Assert(delete_outcome.IsSuccess(), delete_outcome.GetError().GetMessage());
  const auto exist_outcome = GlueTableSchemaExists(client_, database_name_, table_name);
  EXPECT_FALSE(exist_outcome.IsSuccess());
}

TEST_F(AwsGlueUtilsTest, UpdateTableSchemaAndVersions) {
  const std::string table_name = RandomString(8, kCharacterSetLower);
  std::map<std::string, std::string> parameters;
  parameters["A"] = "a";

  const auto create_outcome = CreateGlueTableSchema(
      client_, database_name_, table_name, Aws::Glue::Model::StorageDescriptor().WithLocation("A"), parameters);
  Assert(create_outcome.IsSuccess(), create_outcome.GetError().GetMessage());

  Aws::Glue::Model::TableInput update;
  update.WithName(table_name).WithStorageDescriptor(Aws::Glue::Model::StorageDescriptor().WithLocation("B"));
  const auto update_outcome = UpdateGlueTableSchema(client_, database_name_, update);
  Assert(update_outcome.IsSuccess(), update_outcome.GetError().GetMessage());

  const auto table = GlueTableSchema(client_, database_name_, table_name);
  EXPECT_EQ(table.GetVersionId(), "1");
  EXPECT_TRUE(table.GetParameters().empty());  // Parameters have not been set again
  EXPECT_EQ(table.GetStorageDescriptor().GetLocation(), "B");

  const auto table_versions = GlueTableSchemaVersions(client_, database_name_, table_name);
  EXPECT_EQ(table_versions.size(), 2);
}

TEST_F(AwsGlueUtilsTest, TableSchemas) {
  const size_t num_tables = 100;
  std::set<std::string> table_names;
  for (size_t i = 0; i < num_tables; ++i) {
    const std::string table_name = RandomString(8, kCharacterSetLower);
    table_names.insert(table_name);
    const auto create_outcome =
        CreateGlueTableSchema(client_, database_name_, table_name, Aws::Glue::Model::StorageDescriptor());
    Assert(create_outcome.IsSuccess(), create_outcome.GetError().GetMessage());
  }

  const auto tables = GlueTableSchemas(client_, database_name_);
  EXPECT_EQ(tables.size(), num_tables);
  for (const auto& table : tables) {
    EXPECT_TRUE(table_names.find(table.GetName()) != table_names.end());
  }
}

TEST(GlueUtilsTest, TestErrorTranslation) {
  const std::map<StorageErrorType, std::vector<Aws::Glue::GlueErrors>> mapping = {
      {StorageErrorType::kInvalidArgument,
       {Aws::Glue::GlueErrors::INCOMPLETE_SIGNATURE, Aws::Glue::GlueErrors::INVALID_ACTION,
        Aws::Glue::GlueErrors::INVALID_PARAMETER_COMBINATION, Aws::Glue::GlueErrors::INVALID_PARAMETER_VALUE,
        Aws::Glue::GlueErrors::INVALID_QUERY_PARAMETER, Aws::Glue::GlueErrors::INVALID_SIGNATURE,
        Aws::Glue::GlueErrors::MALFORMED_QUERY_STRING, Aws::Glue::GlueErrors::MISSING_ACTION,
        Aws::Glue::GlueErrors::MISSING_PARAMETER, Aws::Glue::GlueErrors::OPT_IN_REQUIRED,
        Aws::Glue::GlueErrors::REQUEST_EXPIRED, Aws::Glue::GlueErrors::REQUEST_TIME_TOO_SKEWED,
        Aws::Glue::GlueErrors::INVALID_INPUT, Aws::Glue::GlueErrors::VERSION_MISMATCH,
        Aws::Glue::GlueErrors::IDEMPOTENT_PARAMETER_MISMATCH}},
      {StorageErrorType::kInternalError,
       {Aws::Glue::GlueErrors::INTERNAL_FAILURE, Aws::Glue::GlueErrors::SERVICE_UNAVAILABLE}},
      {StorageErrorType::kAlreadyExist, {Aws::Glue::GlueErrors::ALREADY_EXISTS}},
      {StorageErrorType::kPermissionDenied,
       {Aws::Glue::GlueErrors::ACCESS_DENIED, Aws::Glue::GlueErrors::INVALID_ACCESS_KEY_ID,
        Aws::Glue::GlueErrors::INVALID_CLIENT_TOKEN_ID, Aws::Glue::GlueErrors::MISSING_AUTHENTICATION_TOKEN,
        Aws::Glue::GlueErrors::SIGNATURE_DOES_NOT_MATCH, Aws::Glue::GlueErrors::UNRECOGNIZED_CLIENT,
        Aws::Glue::GlueErrors::VALIDATION}},
      {StorageErrorType::kTemporary, {Aws::Glue::GlueErrors::SLOW_DOWN, Aws::Glue::GlueErrors::THROTTLING}},
      {StorageErrorType::kIOError, {Aws::Glue::GlueErrors::REQUEST_TIMEOUT, Aws::Glue::GlueErrors::NETWORK_CONNECTION}},
      {StorageErrorType::kNotFound,
       {Aws::Glue::GlueErrors::RESOURCE_NOT_FOUND, Aws::Glue::GlueErrors::ENTITY_NOT_FOUND,
        Aws::Glue::GlueErrors::NO_SCHEDULE}},
      {StorageErrorType::kUnknown, {Aws::Glue::GlueErrors::UNKNOWN}}};

  for (const auto& check : mapping) {
    for (const auto& error : check.second) {
      ASSERT_EQ(TranslateGlueError(error), check.first);
    }
  }
}

}  // namespace skyrise
