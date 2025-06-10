#include "glue_catalog.hpp"

#include <magic_enum/magic_enum.hpp>

#include "client/coordinator_client.hpp"
#include "glue_utils.hpp"
#include "storage/backend/s3_storage.hpp"
#include "utils/json.hpp"
#include "utils/string.hpp"

namespace skyrise {

namespace {

inline const std::string kManifestRootKey = "SkyriseManifestRoot";
inline const std::string kKeyConstraintRootKey = "SkyriseKeyConstraintRoot";
inline const std::string kKeyConstraintColumnIdsKey = "columnIds";
inline const std::string kKeyConstraintTypeKey = "type";
inline const std::string kNullableKey = "Nullable";

// Possible Glue data types (selectable in the AWS web console): [array, bigint, binary, boolean, char, date, decimal,
// double, float, int, interval, map, set, smallint, string, struct, timestamp, tinyint, union, varchar].
// TODO(tobodner): How do we handle types such as set or timestamp?
const std::unordered_map<std::string, DataType> kGlueDataTypeStringToDataType({
    {"tinyint", DataType::kInt},
    {"smallint", DataType::kInt},
    {"int", DataType::kInt},
    {"bigint", DataType::kLong},
    {"float", DataType::kFloat},
    {"double", DataType::kDouble},
    {"char", DataType::kString},
    {"string", DataType::kString},
    {"varchar", DataType::kString},
});

const std::unordered_map<DataType, std::string> kDataTypeToGlueDataTypeString({
    {DataType::kDouble, "double"},
    {DataType::kFloat, "float"},
    {DataType::kInt, "int"},
    {DataType::kLong, "bigint"},
    {DataType::kString, "string"},
});

std::string DeriveGlueDataType(const DataType& data_type) {
  const auto match = kDataTypeToGlueDataTypeString.find(data_type);
  DebugAssert(match != kDataTypeToGlueDataTypeString.end(),
              "Missing mapping to Glue data type for: " + std::string(magic_enum::enum_name(data_type)));
  return match->second;
}

DataType DeriveDataType(const std::string& glue_data_type) {
  const auto potential_match = kGlueDataTypeStringToDataType.find(glue_data_type);
  DebugAssert(potential_match != kGlueDataTypeStringToDataType.end(),
              "Missing mapping to data type for Glue data type: " + glue_data_type);
  return potential_match->second;
}

std::shared_ptr<const TableSchema> TableSchemaFromGlueTable(const Aws::Glue::Model::Table& glue_table) {
  TableColumnDefinitions schema;

  for (const auto& column : glue_table.GetStorageDescriptor().GetColumns()) {
    const auto data_type = DeriveDataType(column.GetType());
    const auto is_nullable = column.GetParameters().at(kNullableKey) != "0";
    schema.emplace_back(column.GetName(), data_type, is_nullable);
  }

  const auto table_schema = std::make_shared<TableSchema>(schema);

  const auto potential_key_constraint_iter = glue_table.GetParameters().find(kKeyConstraintRootKey);
  if (potential_key_constraint_iter != glue_table.GetParameters().end()) {
    const Aws::Utils::Json::JsonValue json(potential_key_constraint_iter->second);
    const auto key_constraints_array = json.View().AsArray();
    for (size_t i = 0; i < key_constraints_array.GetLength(); ++i) {
      const auto key_constraint_json = key_constraints_array.GetItem(i);
      const std::vector<ColumnId> column_ids =
          JsonArrayToVector<ColumnId>(key_constraint_json.GetArray(kKeyConstraintColumnIdsKey));
      const auto type = magic_enum::enum_cast<KeyConstraintType>(key_constraint_json.GetString(kKeyConstraintTypeKey));
      auto key_constraint =
          TableKeyConstraint(std::unordered_set<ColumnId>(column_ids.begin(), column_ids.end()), type.value());

      table_schema->AddKeyConstraint(key_constraint);
    }
  }

  return table_schema;
}

}  // namespace

GlueCatalog::GlueCatalog(std::shared_ptr<CoordinatorClient> client, std::string database_name)
    : client_(std::move(client)), glue_client_(client_->GetGlueClient()), database_name_(std::move(database_name)) {
  const auto outcome = CreateGlueDatabaseSchema(glue_client_, database_name_);
  Assert(outcome.IsSuccess() || outcome.GetError().GetErrorType() == Aws::Glue::GlueErrors::ALREADY_EXISTS,
         outcome.GetError().GetMessage());
}

const std::string& GlueCatalog::TableBucketName(const std::string& table_name) const {
  return LoadTableMetadata(table_name)->location->bucket_name;
}

bool GlueCatalog::TableExists(const std::string& table_name) const {
  const auto lc_table_name = ToLowerCase(table_name);
  return cache_.contains(lc_table_name) ||
         GlueTableSchemaExists(glue_client_, database_name_, lc_table_name).IsSuccess();
}

std::vector<Aws::Glue::Model::TableVersion> GlueCatalog::TableVersions(const std::string& table_name) const {
  return GlueTableSchemaVersions(glue_client_, database_name_, table_name);
}

std::shared_ptr<const TableSchema> GlueCatalog::GetTableSchema(const std::string& table_name) const {
  return LoadTableMetadata(table_name)->table_schema;
}

std::vector<std::string> GlueCatalog::Tables() const {
  const Aws::Vector<Aws::Glue::Model::Table> glue_tables = GlueTableSchemas(glue_client_, database_name_);

  std::vector<std::string> table_names;
  table_names.reserve(glue_tables.size());
  for (const auto& glue_table : glue_tables) {
    const auto table = ToTableMetadata(glue_table);
    cache_.try_emplace(table->table_name, table);
    table_names.emplace_back(table->table_name);
  }

  return table_names;
}

const std::vector<TablePartition>& GlueCatalog::GetTablePartitions(const std::string& table_name) const {
  const auto& table = LoadTableMetadata(table_name);

  if (table->partitions.empty()) {
    const auto table_storage = std::make_shared<S3Storage>(client_->GetS3Client(), table->location->bucket_name);
    const auto listing_outcome = table_storage->List(table->location->identifier);
    Assert(!listing_outcome.second.IsError(), listing_outcome.second.GetMessage());

    std::vector<TablePartition>& partitions = table->partitions;
    partitions.reserve(listing_outcome.first.size());

    for (const auto& status : listing_outcome.first) {
      partitions.push_back(TablePartition::FromObjectStatus(status));
    }
  }

  return table->partitions;
}

std::shared_ptr<GlueCatalog::TableMetadata> GlueCatalog::ToTableMetadata(const Aws::Glue::Model::Table& glue_table) {
  const auto table_schema = TableSchemaFromGlueTable(glue_table);
  const auto& table_parameter = glue_table.GetParameters();

  const auto location = std::make_shared<ObjectReference>(glue_table.GetStorageDescriptor().GetLocation());

  std::shared_ptr<const ObjectReference> manifest;
  const auto potential_manifest = table_parameter.find(kManifestRootKey);
  if (potential_manifest != table_parameter.cend()) {
    auto reference = ObjectReference(Aws::Utils::Json::JsonValue(potential_manifest->second));
    manifest = std::make_shared<ObjectReference>(reference);
  }

  return std::make_shared<TableMetadata>(TableMetadata{.table_name = glue_table.GetName(),
                                                       .table_version = glue_table.GetVersionId(),
                                                       .table_schema = table_schema,
                                                       .location = location,
                                                       .manifest = manifest,
                                                       .partitions = {}});
}

const std::shared_ptr<GlueCatalog::TableMetadata>& GlueCatalog::LoadTableMetadata(const std::string& table_name) const {
  const auto lc_table_name = ToLowerCase(table_name);
  const auto potential_table = cache_.find(lc_table_name);
  if (potential_table != cache_.cend()) {
    return potential_table->second;
  }

  auto glue_table = GlueTableSchema(glue_client_, database_name_, lc_table_name);
  auto table = ToTableMetadata(glue_table);

  return cache_.emplace(table->table_name, table).first->second;
}

void GlueCatalog::DeleteDatabase() {
  const auto outcome = DeleteGlueDatabaseSchema(glue_client_, database_name_);
  Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  cache_.clear();
}

std::string GlueCatalog::GetDatabaseName() const { return database_name_; };

void GlueCatalog::AddTableMetadata(const std::string& table_name,
                                   const std::shared_ptr<const TableSchema>& table_schema,
                                   const ObjectReference& metadata) {
  Aws::Glue::Model::StorageDescriptor storage_descriptor;
  std::map<std::string, std::string> parameters;

  // (1) Set data location.
  storage_descriptor.SetLocation(metadata.S3Uri());

  // (2) Set table column definitions.
  for (size_t column_id = 0; column_id < table_schema->TableColumnCount(); ++column_id) {
    const auto column_definition = table_schema->GetTableColumnDefinition(column_id);

    Aws::Glue::Model::Column column;
    column.WithName(column_definition.name)
        .WithType(DeriveGlueDataType(column_definition.data_type))
        .AddParameters(kNullableKey, column_definition.nullable ? "1" : "0");

    storage_descriptor.AddColumns(column);
  }

  // (3) Set table key constraints as table parameters.
  if (!table_schema->KeyConstraints().empty()) {
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> constraint_array(table_schema->KeyConstraints().size());
    for (size_t i = 0; i < table_schema->KeyConstraints().size(); ++i) {
      const auto& constraint = table_schema->KeyConstraints()[i];
      constraint_array[i]
          .WithString(kKeyConstraintTypeKey, std::string(magic_enum::enum_name(constraint.KeyType())))
          .WithArray(kKeyConstraintColumnIdsKey, VectorToJsonArray(std::vector<ColumnId>(
                                                     constraint.ColumnIds().begin(), constraint.ColumnIds().end())));
    }

    parameters.emplace(kKeyConstraintRootKey,
                       Aws::Utils::Json::JsonValue().AsArray(constraint_array).View().WriteCompact());
  }

  const auto outcome = CreateGlueTableSchema(glue_client_, database_name_, table_name, storage_descriptor, parameters);
  Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
}

void GlueCatalog::DeleteTableMetadata(const std::string& table_name) {
  const auto lc_table_name = ToLowerCase(table_name);
  const auto outcome = DeleteGlueTableSchema(glue_client_, database_name_, lc_table_name);
  Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  cache_.erase(lc_table_name);
}

void GlueCatalog::AddTableManifest(const std::string& table_name, const ObjectReference& manifest) {
  Assert(TableExists(table_name), "The table does not exist.");

  // We need the newest version of the table to update it.
  const auto table = GlueTableSchema(glue_client_, database_name_, table_name);
  auto input = Aws::Glue::Model::TableInput(table.Jsonize())
                   .AddParameters(kManifestRootKey, manifest.Serialize().View().WriteCompact());

  UpdateGlueTableSchema(glue_client_, database_name_, input);
}

const std::shared_ptr<const ObjectReference>& GlueCatalog::TableManifest(const std::string& table_name) const {
  return LoadTableMetadata(table_name)->manifest;
}

}  // namespace skyrise
