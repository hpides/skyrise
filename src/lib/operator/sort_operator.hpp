/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "operator/abstract_operator.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * Sorts a table by one or multiple columns in a stable fashion.
 */
class SortOperator : public AbstractOperator {
 public:
  SortOperator(const std::shared_ptr<const AbstractOperator> input_operator,
               const std::vector<SortColumnDefinition>& sort_definitions);

  const std::vector<SortColumnDefinition>& SortDefinitions() const;

  const std::string& Name() const override;

 protected:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context = nullptr) override;

  /**
   * Given an unsorted_table and a position_list defining the output order, this materializes all columns in the table.
   */
  std::shared_ptr<Table> MaterializeOutputTable(const std::shared_ptr<const Table>& unsorted_table,
                                                const RowIdPositionList& position_list,
                                                const std::shared_ptr<FragmentScheduler>& fragment_scheduler) const;

  template <typename SortColumnType>
  class SortImplementation;

  const std::vector<SortColumnDefinition> sort_definitions_;
};

}  // namespace skyrise
