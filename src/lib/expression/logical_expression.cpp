/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "logical_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "utils/assert.hpp"

namespace skyrise {

std::ostream& operator<<(std::ostream& stream, const LogicalOperator logical_operator) {
  switch (logical_operator) {
    case LogicalOperator::kAnd:
      stream << "AND";
      break;
    case LogicalOperator::kOr:
      stream << "OR";
      break;
  }
  return stream;
}

LogicalExpression::LogicalExpression(const LogicalOperator init_logical_operator,
                                     const std::shared_ptr<AbstractExpression>& left_operand,
                                     const std::shared_ptr<AbstractExpression>& right_operand)
    : AbstractExpression(ExpressionType::kLogical, {left_operand, right_operand}),
      logical_operator_(init_logical_operator) {}

const std::shared_ptr<AbstractExpression>& LogicalExpression::LeftOperand() const { return arguments_[0]; }

const std::shared_ptr<AbstractExpression>& LogicalExpression::RightOperand() const { return arguments_[1]; }

std::shared_ptr<AbstractExpression> LogicalExpression::DeepCopy() const {
  return std::make_shared<LogicalExpression>(logical_operator_, LeftOperand()->DeepCopy(), RightOperand()->DeepCopy());
}

std::string LogicalExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << EncloseArgument(*LeftOperand(), mode) << " " << logical_operator_ << " "
         << EncloseArgument(*RightOperand(), mode);
  return stream.str();
}

DataType LogicalExpression::GetDataType() const {
  // TODO(TheoRadig): Revisit DataType once the ExpressionEvaluator is introduced.
  return DataType::kInt;
}

bool LogicalExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const LogicalExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  return logical_operator_ == static_cast<const LogicalExpression&>(expression).logical_operator_;
}

size_t LogicalExpression::ShallowHash() const { return boost::hash_value(static_cast<size_t>(logical_operator_)); }

ExpressionPrecedence LogicalExpression::Precedence() const { return ExpressionPrecedence::kLogical; }

}  // namespace skyrise
