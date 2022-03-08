#pragma once

#include <memory>
#include <unordered_map>

#include "types.hpp"

namespace skyrise {

class AbstractLqpNode;

/**
 * Wraps an LQP, which can, for example, originate from a SQL View or a SQL WITH description.
 * When translating SQL, copies of this wrapper's LQP are made to create new, bigger LQPs in the SqlTranslator.
 */
class LqpWrapper {
 public:
  LqpWrapper(const std::shared_ptr<AbstractLqpNode>& lqp, std::unordered_map<ColumnId, std::string> column_names);

  std::shared_ptr<LqpWrapper> DeepCopy() const;
  bool DeepEquals(const LqpWrapper& other) const;

  const std::shared_ptr<AbstractLqpNode> lqp_;
  const std::unordered_map<ColumnId, std::string> column_names_;
};

}  // namespace skyrise
