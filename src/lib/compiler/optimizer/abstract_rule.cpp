#include "abstract_rule.hpp"

#include "utils/assert.hpp"

namespace skyrise {

void AbstractRule::ApplyTo(const std::shared_ptr<AbstractLqpNode>& /*lqp_root*/) const {
  Fail(Name() + " is not implemented for logical query plans.");
}

void AbstractRule::ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& /*pqp_root*/) const {
  Fail(Name() + " is not implemented for physical query plans.");
}

}  // namespace skyrise
