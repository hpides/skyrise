#pragma once

#include <memory>
#include <string>

namespace skyrise {
class AbstractLqpNode;
class AbstractOperatorProxy;

class AbstractRule {
 public:
  virtual ~AbstractRule() = default;

  virtual const std::string& Name() const = 0;

  virtual void ApplyTo(const std::shared_ptr<AbstractLqpNode>& lqp_root) const;
  virtual void ApplyTo(const std::shared_ptr<AbstractOperatorProxy>& pqp_root) const;
};

}  // namespace skyrise
