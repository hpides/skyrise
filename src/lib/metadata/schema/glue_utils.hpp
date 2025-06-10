#pragma once

#include <aws/glue/GlueClient.h>
#include <aws/glue/model/TableInput.h>

#include "storage/backend/errors.hpp"
#include "table_schema.hpp"

namespace skyrise {

StorageErrorType TranslateGlueError(const Aws::Glue::GlueErrors error);

Aws::Glue::Model::CreateDatabaseOutcome CreateGlueDatabaseSchema(
    const std::shared_ptr<const Aws::Glue::GlueClient>& client, const std::string& database_name);
Aws::Glue::Model::DeleteDatabaseOutcome DeleteGlueDatabaseSchema(
    const std::shared_ptr<const Aws::Glue::GlueClient>& client, const std::string& database_name);

Aws::Glue::Model::CreateTableOutcome CreateGlueTableSchema(
    const std::shared_ptr<const Aws::Glue::GlueClient>& client, const std::string& database_name,
    const std::string& table_name, const Aws::Glue::Model::StorageDescriptor& storage_descriptor,
    const std::map<std::string, std::string>& parameters = {});

Aws::Glue::Model::DeleteTableOutcome DeleteGlueTableSchema(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                                           const std::string& database_name,
                                                           const std::string& table_name);

Aws::Glue::Model::Table GlueTableSchema(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                        const std::string& database_name, const std::string& table_name);

std::vector<Aws::Glue::Model::Table> GlueTableSchemas(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                                      const std::string& database_name);

Aws::Glue::Model::GetTableOutcome GlueTableSchemaExists(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                                        const std::string& database_name,
                                                        const std::string& table_name);

std::vector<Aws::Glue::Model::TableVersion> GlueTableSchemaVersions(
    const std::shared_ptr<const Aws::Glue::GlueClient>& client, const std::string& database_name,
    const std::string& table_name);

/**
 * The passed @param table_input will replace the latest table's information and should therefore contain also the
 * non-updated attributes.
 */
Aws::Glue::Model::UpdateTableOutcome UpdateGlueTableSchema(const std::shared_ptr<const Aws::Glue::GlueClient>& client,
                                                           const std::string& database_name,
                                                           const Aws::Glue::Model::TableInput& table_input);

}  // namespace skyrise
