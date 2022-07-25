#pragma once

#include <string>

#include "compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "expression/pqp_column_expression.hpp"

namespace skyrise {

/**
 * @return @param count ObjectReferences for testing purposes from @param key_prefix.
 */
std::shared_ptr<ImportOperatorProxy> CreateMockObjectReferences(const std::string& key_prefix, size_t count);

/**
 * @return a TpchTable PqpColumnExpression for the given @param column_name.
 */
std::shared_ptr<PqpColumnExpression> TpchPqpColumn(const std::string column_name);

/**
 * Resolves TpchTable by the given @param column_names and
 * @returns an ImportOperatorProxy with the according TpchTable import column ids and given @param object_references.
 */
std::shared_ptr<ImportOperatorProxy> TpchImportProxy(const std::vector<std::string> column_names,
                                                                const std::vector<ObjectReference> object_references);

}  // namespace skyrise
