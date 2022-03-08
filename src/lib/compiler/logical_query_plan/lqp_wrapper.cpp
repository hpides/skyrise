#include "lqp_wrapper.hpp"

#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "compiler/logical_query_plan/lqp_utils.hpp"

namespace skyrise {

LqpWrapper::LqpWrapper(const std::shared_ptr<AbstractLqpNode>& lqp,
                       std::unordered_map<ColumnId, std::string> column_names = {})
    : lqp_(lqp), column_names_(std::move(column_names)) {}

std::shared_ptr<LqpWrapper> LqpWrapper::DeepCopy() const {
  return std::make_shared<LqpWrapper>(lqp_->DeepCopy(), column_names_);
}

bool LqpWrapper::DeepEquals(const LqpWrapper& other) const {
  return *lqp_ == *other.lqp_ && column_names_ == other.column_names_;
}

}  // namespace skyrise
