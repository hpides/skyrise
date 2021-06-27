#include "abstract_operator.hpp"

#include "utils/assert.hpp"

namespace skyrise {

AbstractOperator::AbstractOperator(OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                   const std::shared_ptr<const AbstractOperator>& right)
    : type_(type), left_input_(left), right_input_(right) {}

OperatorType AbstractOperator::Type() const { return type_; }

void AbstractOperator::Execute() {
  DebugAssert(!left_input_ || left_input_->GetOutput(), "Left input has not yet been executed.");
  DebugAssert(!right_input_ || right_input_->GetOutput(), "Right input has not yet been executed.");

  output_ = OnExecute();
  OnCleanup();
}

std::shared_ptr<const Table> AbstractOperator::GetOutput() const { return output_; }

void AbstractOperator::ClearOutput() { output_ = nullptr; }

std::string AbstractOperator::Description(DescriptionMode /*description_mode*/) const { return Name(); }

std::shared_ptr<const Table> AbstractOperator::LeftInputTable() const { return left_input_->GetOutput(); }

std::shared_ptr<const Table> AbstractOperator::RightInputTable() const { return right_input_->GetOutput(); }

std::shared_ptr<AbstractOperator> AbstractOperator::MutableLeftInput() const {
  return std::const_pointer_cast<AbstractOperator>(left_input_);
}

std::shared_ptr<AbstractOperator> AbstractOperator::MutableRightInput() const {
  return std::const_pointer_cast<AbstractOperator>(right_input_);
}

std::shared_ptr<const AbstractOperator> AbstractOperator::LeftInput() const { return left_input_; }
std::shared_ptr<const AbstractOperator> AbstractOperator::RightInput() const { return right_input_; }

void AbstractOperator::OnCleanup() {}

}  // namespace skyrise
