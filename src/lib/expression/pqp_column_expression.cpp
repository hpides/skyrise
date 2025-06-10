/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "pqp_column_expression.hpp"

#include <boost/container_hash/hash.hpp>

namespace skyrise {

std::shared_ptr<PqpColumnExpression> PqpColumnExpression::FromTable(const Table& table,
                                                                    const std::string& column_name) {
  const auto column_id = table.ColumnIdByName(column_name);
  return std::make_shared<PqpColumnExpression>(column_id, table.ColumnDataType(column_id),
                                               table.ColumnIsNullable(column_id), column_name);
}

std::shared_ptr<PqpColumnExpression> PqpColumnExpression::FromTable(const Table& table, const ColumnId column_id) {
  return PqpColumnExpression::FromTable(table, table.ColumnName(column_id));
}

PqpColumnExpression::PqpColumnExpression(const ColumnId column_id, const DataType data_type, const bool is_nullable,
                                         const std::string& column_name)
    : AbstractExpression(ExpressionType::kPqpColumn, {}),
      column_id_(column_id),
      data_type_(data_type),
      is_nullable_(is_nullable),
      column_name_(column_name) {}

std::shared_ptr<AbstractExpression> PqpColumnExpression::DeepCopy() const {
  return std::make_shared<PqpColumnExpression>(column_id_, data_type_, is_nullable_, column_name_);
}

std::string PqpColumnExpression::Description(const DescriptionMode /*mode*/) const { return column_name_; }

ColumnId PqpColumnExpression::GetColumnId() const { return column_id_; }

DataType PqpColumnExpression::GetDataType() const { return data_type_; }

bool PqpColumnExpression::IsNullable() const { return is_nullable_; }

std::string PqpColumnExpression::GetColumnName() const { return column_name_; }

bool PqpColumnExpression::RequiresComputation() const { return false; }

bool PqpColumnExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const PqpColumnExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  const auto& pqp_column_expression = static_cast<const PqpColumnExpression&>(expression);
  return column_id_ == pqp_column_expression.column_id_ && data_type_ == pqp_column_expression.data_type_ &&
         is_nullable_ == pqp_column_expression.is_nullable_ && column_name_ == pqp_column_expression.column_name_;
}

size_t PqpColumnExpression::ShallowHash() const { return boost::hash_value(static_cast<size_t>(column_id_)); }

}  // namespace skyrise
