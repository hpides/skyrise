#pragma once

#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "compiler/optimizer/abstract_rule.hpp"

namespace skyrise {

class LqpAverageRewriteRule : public AbstractRule {
 public:
  const std::string& Name() const override;

  void ApplyTo(const std::shared_ptr<AbstractLqpNode>& lqp_root) const override;
};

}  // namespace skyrise
