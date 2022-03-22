/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_operator.hpp"

namespace {

const std::string kName = "TableWrapper";

}  // namespace

namespace skyrise {

/**
 * Operator that wraps a table.
 */
class TableWrapper : public AbstractOperator {
 public:
  explicit TableWrapper(std::shared_ptr<const Table> table)
      : AbstractOperator(OperatorType::kImport), table_(std::move(table)) {}

  const std::string& Name() const override { return kName; }

 protected:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& /*operator_execution_context*/) override {
    return table_;
  }

  const std::shared_ptr<const Table> table_;
};

}  // namespace skyrise
