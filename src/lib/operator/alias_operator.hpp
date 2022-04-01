/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_operator.hpp"

namespace skyrise {

/**
 * Forward the input columns in the specified order with updated column names.
 */
class AliasOperator : public AbstractOperator {
 public:
  AliasOperator(std::shared_ptr<const AbstractOperator> input_operator, const std::vector<ColumnId>& column_ids,
                const std::vector<std::string>& aliases);

  const std::string& Name() const override;

 protected:
  std::shared_ptr<const Table> OnExecute(const std::shared_ptr<OperatorExecutionContext>& context) override;

 private:
  const std::vector<ColumnId> column_ids_;
  const std::vector<std::string> aliases_;
};

}  // namespace skyrise
