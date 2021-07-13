#pragma once

#include <aws/core/utils/json/JsonSerializer.h>

#include "abstract_operator_proxy.hpp"

namespace skyrise {
/**
 * PqpSerializer serializes a DAG of operator proxies to a string representation of a JSON object. This object has
 * following two keys:
 * - "root_identity": A string containing the identity of the root operator.
 * - "operators": An object where each attribute represents an operator. The attribute key is a string containing the
 * identity of the described operator.
 *
 * Each operator is serialized as an object. While the attributes vary depending on the type of the operator some
 * attributes are valid for all operators. These are:
 * - "operator_type": A string mapping to an enum value of OperatorType.
 * - "left_input_identity": An optional string containing the identity of the left input operator.
 * - "right_input_identity": An optional string containing the identity of the right input operator.
 */
class PqpSerializer {
 public:
  PqpSerializer(std::shared_ptr<const AbstractOperatorProxy> root_operator_proxy);
  std::string Serialize();

 private:
  void RecursiveSerialize(const std::shared_ptr<const AbstractOperatorProxy>& operator_proxy);

  std::shared_ptr<const AbstractOperatorProxy> root_operator_proxy_;
  Aws::Utils::Json::JsonValue id_to_json_;
};

}  // namespace skyrise
