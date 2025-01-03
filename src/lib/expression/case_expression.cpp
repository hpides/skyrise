#include "case_expression.hpp"

#include <sstream>

#include "expression_utils.hpp"

namespace skyrise {

CaseExpression::CaseExpression(const std::shared_ptr<AbstractExpression>& when,
                               const std::shared_ptr<AbstractExpression>& then,
                               const std::shared_ptr<AbstractExpression>& otherwise)
    : AbstractExpression(ExpressionType::kCase, {when, then, otherwise}) {}

const std::shared_ptr<AbstractExpression>& CaseExpression::When() const { return arguments_[0]; }

const std::shared_ptr<AbstractExpression>& CaseExpression::Then() const { return arguments_[1]; }

const std::shared_ptr<AbstractExpression>& CaseExpression::Otherwise() const { return arguments_[2]; }

std::string CaseExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;

  stream << "CASE WHEN " << When()->Description(mode) << " THEN " << Then()->Description(mode) << " ELSE "
         << Otherwise()->Description(mode) << " END";

  return stream.str();
}

DataType CaseExpression::GetDataType() const {
  return ExpressionCommonType(Then()->GetDataType(), Otherwise()->GetDataType());
}

std::shared_ptr<AbstractExpression> CaseExpression::DeepCopy() const {
  return std::make_shared<CaseExpression>(arguments_[0]->DeepCopy(), arguments_[1]->DeepCopy(),
                                          arguments_[2]->DeepCopy());
}

bool CaseExpression::ShallowEquals([[maybe_unused]] const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const CaseExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return true;
}

size_t CaseExpression::ShallowHash() const {
  // CaseExpression introduces no additional data fields. Therefore, we return a constant hash value.
  return 0;
}

}  // namespace skyrise
