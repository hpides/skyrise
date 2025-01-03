#include "pqp_utils.hpp"

#include "utils/assert.hpp"

namespace skyrise {

std::vector<std::shared_ptr<AbstractOperatorProxy>> PqpFindLeaves(const std::shared_ptr<AbstractOperatorProxy>& pqp) {
  std::vector<std::shared_ptr<AbstractOperatorProxy>> leaves;
  VisitPqp(pqp, [&](const auto& operator_proxy) {
    if (operator_proxy->LeftInput()) {
      return PqpVisitation::kVisitInputs;
    } else {
      leaves.push_back(operator_proxy);
    }
    return PqpVisitation::kDoNotVisitInputs;
  });
  Assert(!leaves.empty(), "Did expect to find at least one leaf operator proxy!");
  return leaves;
}

void PrefixOperatorProxyIdentities(const std::shared_ptr<AbstractOperatorProxy>& pqp, const std::string& prefix) {
  VisitPqp(pqp, [&](const auto& operator_proxy) {
    operator_proxy->PrefixIdentity(prefix);
    return PqpVisitation::kVisitInputs;
  });
}

}  // namespace skyrise
