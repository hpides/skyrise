#include "pqp_test_utils.hpp"

#include "constants.hpp"

namespace skyrise {

std::shared_ptr<ImportOperatorProxy> CreateMockObjectReferences(const std::string& key_prefix, size_t count) {
  std::vector<ObjectReference>& object_references;
  object_references.reserve(count);

  for (size_t i = 0; i < count; ++i) {
    const std::string key = key_prefix + std::to_string(i) + kOrcExtension;
    object_references.emplace_back("mock_bucket", key, "mock_etag");
  }

  return object_references;
}

std::shared_ptr<ImportOperatorProxy> CreateTpchImportProxy(const TpchTable tpch_table,
                                                           const std::vector<std::string> column_names,
                                                           const std::vector<ObjectReference> object_references) {
  const TableColumnDefinitions column_definitions = TpchColumnDefinitionsByTable(tpch_table);

  // Derive ColumnIDs from column names.
  std::vector<ColumnID> column_ids;
  for (const std::string& column_name : column_names) {
    ColumnID column_id = kInvalidColumnId;
    for (ColumnID current_column_id = 0; current_column_id < column_definitions.size(); ++current_column_id) {
      if (column_definitions.at(current_column_id).name == column_name) {
        column_id = current_column_id;
        break;
      }
    }
    ASSERT_NE(column_id, kInvalidColumnId);
    column_ids.push_back(column_id);
  }

  // Create proxy & set TPC-H table name as a comment to support debugging.
  const auto import_proxy = ImportOperatorProxy::Make(object_references, column_ids);
  import_proxy->SetComment(std::string(magic_enum::enum_name(tpch_table)));

  return import_proxy;
}

}  // namespace skyrise
