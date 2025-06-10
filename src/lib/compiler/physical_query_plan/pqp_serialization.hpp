#pragma once

#include <aws/core/utils/json/JsonSerializer.h>

#include "operator_proxy/abstract_operator_proxy.hpp"

namespace skyrise {

/**
 * The SerializePqp function takes a PQP or any other DAG of operator proxies and generates a JSON object. The returned
 * string can be transferred across the network to other computer systems. The DeserializePqp function can be used to
 * re-create the original PQP or DAG of operator proxies.
 *
 * Serialization details:
 *  The respective JSON object has the following two keys:
 *   - "operators"                 An object where each attribute represents an operator. The attribute key is a string
 *                                 containing the identity of the described operator.
 *         Each operator is serialized as a flat object. While attributes vary depending on the type of the
 *         operator, some attributes are valid for all operators, for example:
 *          - "operator_type"                  A string mapping to an enum value of OperatorType.
 *          - "left_input_operator_identity"   An optional string containing the identity of the left input operator.
 *          - "right_input_operator_identity"  An optional string containing the identity of the right input operator.
 *
 *   - "root_operator_identity"    A string that identifies the root operator proxy in the "operators" JSON object.
 */
std::string SerializePqp(const std::shared_ptr<const AbstractOperatorProxy>& root_operator_proxy);
std::shared_ptr<AbstractOperatorProxy> DeserializePqp(const std::string& pqp_string);

}  // namespace skyrise
