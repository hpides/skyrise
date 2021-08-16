/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "expression_functional.hpp"

namespace skyrise {

namespace expression_functional {

std::shared_ptr<AbstractExpression> ToExpression(const std::shared_ptr<AbstractExpression>& expression) {
  return expression;
}

std::shared_ptr<ValueExpression> ToExpression(const AllTypeVariant& value) {
  return std::make_shared<ValueExpression>(value);
}

std::shared_ptr<ValueExpression> Value_(const AllTypeVariant& value) {
  return std::make_shared<ValueExpression>(value);
}

std::shared_ptr<ValueExpression> Null_() { return std::make_shared<ValueExpression>(kNullValue); }

std::shared_ptr<PqpColumnExpression> PqpColumn_(const ColumnId column_id, const DataType data_type, const bool nullable,
                                                const std::string& column_name) {
  return std::make_shared<PqpColumnExpression>(column_id, data_type, nullable, column_name);
}

}  // namespace expression_functional

}  // namespace skyrise
