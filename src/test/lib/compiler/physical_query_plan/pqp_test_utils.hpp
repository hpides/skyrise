#pragma once

#include <string>

#include "compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"

namespace skyrise {

/**
 *
 * @param key_prefix
 * @param count
 * @return
 */
std::shared_ptr<ImportOperatorProxy> CreateMockObjectReferences(const std::string& key_prefix, size_t count);

/**
 *
 * @param tpch_table
 * @param column_names
 * @param object_references
 * @return
 */
std::shared_ptr<ImportOperatorProxy> CreateTpchImportProxy(const TpchTable tpch_table,
                                                           const std::vector<std::string> column_names,
                                                           const std::vector<ObjectReference> object_references);

}  // namespace skyrise
