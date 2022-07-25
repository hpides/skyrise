#include "pqp_test_utils.hpp"

#include "constants.hpp"

namespace skyrise {

namespace {

/**
 * @return the according TpchTable, based on the prefix of @param tpch_column_name.
 */
std::unordered_map<std::string, ColumnID> ColumnIdsByColumnNames(const TableColumnDefinitions column_definitions) {
  std::unordered_map<std::string, ColumnID> column_id_by_column_name;
  column_id_by_column_name.reserve(column_definitions.size());
  for (ColumnID i = 0; i < column_definitions.size(); ++i) {
    column_id_by_column_name.emplace(column_definitions[i].name, i);
  }
  return column_id_by_column_name;
}

/**
 * @return the according TpchTable, based on the prefix of @param tpch_column_name.
 */
TpchTable ResolveTpchTable(const std::string& tpch_column_name) {
  switch (tpch_column_name.at(0)) {
    case 'c':
      return TpchTable::kCustomer;
    case 'l':
      return TpchTable::kLineItem;
    case 'o':
      return TpchTable::kOrders;
    case 'p':
      if (tpch_column_name.at(1) == 's') {
        return TpchTable::kPartSupp;
      }
      return TpchTable::kPart;
    case 's':
      return TpchTable::kSupplier;
    default:
      Fail("Could not resolve TpchTable, given the column name '" + tpch_column_name + "'");
  }
}

}  // namespace

std::shared_ptr<ImportOperatorProxy> CreateMockObjectReferences(const std::string& key_prefix, size_t count) {
  std::vector<ObjectReference>& object_references;
  object_references.reserve(count);

  for (size_t i = 0; i < count; ++i) {
    const std::string key = key_prefix + std::to_string(i) + kOrcExtension;
    object_references.emplace_back("mock_bucket", key, "mock_etag");
  }

  return object_references;
}

std::shared_ptr<PqpColumnExpression> TpchPqpColumn(const std::string tpch_column_name) {
  const TpchTable tpch_table = ResolveTpchTable(column_name);

  // Resolve ColumnID
  ColumnID column_id = kInvalidColumnID;
  const TableColumnDefinitions column_definitions = TpchColumnDefinitionsByTable(tpch_table);
  for (ColumnID i = 0; i < column_definitions.size(); ++i) {
    if (column_definitions[i].name != tpch_column_name) {
      continue;
    }
    column_id = i;
    break;
  }
  if (column_id == kInvalidColumnID) {
    Fail("Could not resolve Tpch PQP column definition.");
  }

  // Create PqpColumnExpression
  const auto column_definition = column_definitions[column_id];
  return PqpColumn_(column_id, column_definition.data_type, column_definition.nullable, column_definition.name);
}

std::shared_ptr<ImportOperatorProxy> TpchImportProxy(const std::vector<std::string> column_names,
                                                                const std::vector<ObjectReference> object_references) {
  Assert(!column_names.empty(), "At least one column name must be provided for a TpchTable ImportOperatorProxy.");
  const TpchTable tpch_table = ResolveTpchTable(column_names[0]);

  const TableColumnDefinitions column_definitions = TpchColumnDefinitionsByTable(tpch_table);
  const auto column_id_by_column_name = ColumnIdsByColumnNames(column_definitions);

  // Resolve import ColumnIDs.
  std::vector<ColumnID> import_column_ids;
  for (const std::string& column_name : column_names) {
    const auto column_id_by_column_name_iter = column_id_by_column_name.find(column_name);
    Assert(column_id_by_column_name_iter != column_id_by_column_name.cend(), "Could not resolve ColumnID for column '" + column_name + "'");
    import_column_ids.push_back(column_id);
  }

  // Create proxy & set TPC-H table name as a comment to support debugging.
  const auto import_proxy = ImportOperatorProxy::Make(object_references, import_column_ids);
  import_proxy->SetComment(std::string(magic_enum::enum_name(tpch_table)));

  return import_proxy;
}

}  // namespace skyrise
