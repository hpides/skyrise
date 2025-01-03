#include "glue_utils.hpp"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/glue/model/CreateDatabaseRequest.h>
#include <aws/glue/model/CreateTableRequest.h>
#include <aws/glue/model/DeleteDatabaseRequest.h>
#include <aws/glue/model/DeleteTableRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/GetTableVersionsRequest.h>
#include <aws/glue/model/GetTablesRequest.h>
#include <aws/glue/model/UpdateTableRequest.h>
#include <magic_enum/magic_enum.hpp>

namespace skyrise {

StorageErrorType TranslateGlueError(const Aws::Glue::GlueErrors error) {
  switch (error) {
    case Aws::Glue::GlueErrors::INCOMPLETE_SIGNATURE:
    case Aws::Glue::GlueErrors::INVALID_ACTION:
    case Aws::Glue::GlueErrors::INVALID_PARAMETER_COMBINATION:
    case Aws::Glue::GlueErrors::INVALID_PARAMETER_VALUE:
    case Aws::Glue::GlueErrors::INVALID_QUERY_PARAMETER:
    case Aws::Glue::GlueErrors::INVALID_SIGNATURE:
    case Aws::Glue::GlueErrors::MALFORMED_QUERY_STRING:
    case Aws::Glue::GlueErrors::MISSING_ACTION:
    case Aws::Glue::GlueErrors::MISSING_PARAMETER:
    case Aws::Glue::GlueErrors::OPT_IN_REQUIRED:
    case Aws::Glue::GlueErrors::REQUEST_EXPIRED:
    case Aws::Glue::GlueErrors::REQUEST_TIME_TOO_SKEWED:
    case Aws::Glue::GlueErrors::INVALID_INPUT:
    case Aws::Glue::GlueErrors::VERSION_MISMATCH:
    case Aws::Glue::GlueErrors::IDEMPOTENT_PARAMETER_MISMATCH:
      return StorageErrorType::kInvalidArgument;

    case Aws::Glue::GlueErrors::INTERNAL_FAILURE:
    case Aws::Glue::GlueErrors::SERVICE_UNAVAILABLE:
      return StorageErrorType::kInternalError;

    case Aws::Glue::GlueErrors::ALREADY_EXISTS:
      return StorageErrorType::kAlreadyExist;

    case Aws::Glue::GlueErrors::ACCESS_DENIED:
    case Aws::Glue::GlueErrors::INVALID_ACCESS_KEY_ID:
    case Aws::Glue::GlueErrors::INVALID_CLIENT_TOKEN_ID:
    case Aws::Glue::GlueErrors::MISSING_AUTHENTICATION_TOKEN:
    case Aws::Glue::GlueErrors::SIGNATURE_DOES_NOT_MATCH:
    case Aws::Glue::GlueErrors::UNRECOGNIZED_CLIENT:
    case Aws::Glue::GlueErrors::VALIDATION:
      return StorageErrorType::kPermissionDenied;

    case Aws::Glue::GlueErrors::SLOW_DOWN:
    case Aws::Glue::GlueErrors::THROTTLING:
      return StorageErrorType::kTemporary;

    case Aws::Glue::GlueErrors::NETWORK_CONNECTION:
    case Aws::Glue::GlueErrors::REQUEST_TIMEOUT:
      return StorageErrorType::kIOError;

    case Aws::Glue::GlueErrors::RESOURCE_NOT_FOUND:
    case Aws::Glue::GlueErrors::ENTITY_NOT_FOUND:
    case Aws::Glue::GlueErrors::NO_SCHEDULE:
      return StorageErrorType::kNotFound;

    default:
      return StorageErrorType::kUnknown;
  }
}

Aws::Glue::Model::CreateDatabaseOutcome CreateGlueDatabaseSchema(
    const std::shared_ptr<const Aws::Glue::GlueClient>& client, const std::string& database_name) {
  Aws::Glue::Model::DatabaseInput input;
  input.WithName(database_name);
  Aws::Glue::Model::CreateDatabaseRequest request;
  request.WithDatabaseInput(input);

  return client->CreateDatabase(request);
}

Aws::Glue::Model::DeleteDatabaseOutcome DeleteGlueDatabaseSchema(
    const std::shared_ptr<const Aws::Glue::GlueClient>& client, const std::string& database_name) {
  Aws::Glue::Model::DeleteDatabaseRequest request;
  request.WithName(database_name);

  return client->DeleteDatabase(request);
}

Aws::Glue::Model::CreateTableOutcome CreateGlueTableSchema(
    const std::shared_ptr<const Aws::Glue::GlueClient>& client, const std::string& database_name,
    const std::string& table_name, const Aws::Glue::Model::StorageDescriptor& storage_descriptor,
    const std::map<std::string, std::string>& parameters) {
  Aws::Glue::Model::TableInput input;
  input.WithName(table_name).WithStorageDescriptor(storage_descriptor).WithParameters(parameters);

  Aws::Glue::Model::CreateTableRequest request;
  request.WithDatabaseName(database_name).WithTableInput(input);

  return client->CreateTable(request);
}

Aws::Glue::Model::DeleteTableOutcome DeleteGlueTableSchema(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                                           const std::string& database_name,
                                                           const std::string& table_name) {
  Aws::Glue::Model::DeleteTableRequest request;
  request.WithDatabaseName(database_name).WithName(table_name);

  return client->DeleteTable(request);
}

Aws::Glue::Model::GetTableOutcome GlueTableSchemaExists(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                                        const std::string& database_name,
                                                        const std::string& table_name) {
  Aws::Glue::Model::GetTableRequest request;
  request.WithDatabaseName(database_name).WithName(table_name);

  return client->GetTable(request);
}

Aws::Glue::Model::Table GlueTableSchema(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                        const std::string& database_name, const std::string& table_name) {
  Aws::Glue::Model::GetTableRequest request;
  request.WithDatabaseName(database_name).WithName(table_name);

  Aws::Glue::Model::GetTableOutcome outcome = client->GetTable(request);
  return outcome.GetResultWithOwnership().GetTable();
}

std::vector<Aws::Glue::Model::Table> GlueTableSchemas(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                                      const std::string& database_name) {
  Aws::Glue::Model::GetTablesRequest request;
  request.WithDatabaseName(database_name);

  std::vector<Aws::Glue::Model::Table> tables;
  Aws::Glue::Model::GetTablesOutcome outcome;
  std::string continuation_token;
  do {
    if (!continuation_token.empty()) {
      request.WithNextToken(continuation_token);
    }
    outcome = client->GetTables(request);
    continuation_token = outcome.GetResult().GetNextToken();
    tables.reserve(tables.size() + outcome.GetResult().GetTableList().size());
    for (const auto& table : outcome.GetResult().GetTableList()) {
      tables.push_back(table);
    }

  } while (!continuation_token.empty());

  return tables;
}

Aws::Vector<Aws::Glue::Model::TableVersion> GlueTableSchemaVersions(
    const std::shared_ptr<const Aws::Glue::GlueClient>& client, const std::string& database_name,
    const std::string& table_name) {
  Aws::Glue::Model::GetTableVersionsRequest request;
  request.WithDatabaseName(database_name).WithTableName(table_name);

  const auto outcome = client->GetTableVersions(request);
  return outcome.GetResult().GetTableVersions();
}

Aws::Glue::Model::UpdateTableOutcome UpdateGlueTableSchema(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                                           const std::string& database_name,
                                                           const Aws::Glue::Model::TableInput& table_input) {
  Aws::Glue::Model::UpdateTableRequest request;
  request.WithDatabaseName(database_name).WithTableInput(table_input);

  return client->UpdateTable(request);
}

}  // namespace skyrise
