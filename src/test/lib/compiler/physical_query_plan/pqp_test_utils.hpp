#pragma once

#include <string>

#include "compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp"
#include "expression/pqp_column_expression.hpp"

namespace skyrise {

/**
 * @return @param count ObjectReferences for testing purposes from @param key_prefix.
 */
std::vector<ObjectReference> CreateMockObjectReferences(const std::string& key_prefix, size_t count);

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

/**
 * TODO
 * @param lineitem_mock_objects_count
 * @param combiner_stages_worker_count
 * @return
 */
std::shared_ptr<ExportOperatorProxy> CreateTpchQ1Pqp(size_t lineitem_mock_objects_count,
                                                     std::vector<size_t> combiner_stages_worker_count);

}  // namespace skyrise
