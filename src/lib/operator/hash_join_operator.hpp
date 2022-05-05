#pragma once

#include "abstract_operator.hpp"
#include "join_operator_predicate.hpp"
#include "types.hpp"

namespace skyrise {

class HashJoinOperator : public AbstractOperator {
 public:
  using PositionLists = std::vector<std::vector<std::pair<RowId, ChunkOffset>>>;

  HashJoinOperator(std::shared_ptr<const AbstractOperator> left_input,
                   std::shared_ptr<const AbstractOperator> right_input,
                   std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode);

  const std::string& Name() const override;

 private:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) override;

  const std::shared_ptr<JoinOperatorPredicate> predicate_;
  const JoinMode join_mode_;
};

}  // namespace skyrise
