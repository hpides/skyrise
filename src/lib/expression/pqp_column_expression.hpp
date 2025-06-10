/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"
#include "storage/table/table.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * Wraps a ColumnId and its associated data type, nullability, and column name.
 */
class PqpColumnExpression : public AbstractExpression {
 public:
  static std::shared_ptr<PqpColumnExpression> FromTable(const Table& table, const std::string& column_name);
  static std::shared_ptr<PqpColumnExpression> FromTable(const Table& table, const ColumnId column_id);

  PqpColumnExpression(const ColumnId column_id, const DataType data_type, const bool is_nullable,
                      const std::string& column_name);

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  bool RequiresComputation() const override;
  std::string Description(const DescriptionMode mode) const override;
  ColumnId GetColumnId() const;
  DataType GetDataType() const override;
  bool IsNullable() const;
  std::string GetColumnName() const;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;

  const ColumnId column_id_;
  const DataType data_type_;
  const bool is_nullable_;
  const std::string column_name_;
};

}  // namespace skyrise
