/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>

#include "operator/execution_context.hpp"
#include "storage/table/table.hpp"
#include "types.hpp"

namespace skyrise {

enum class OperatorType {
  kAggregate,
  kAlias,
  kExchange,  // Control operator, not used during execution.
  kExport,
  kFilter,
  kImport,
  kHashJoin,
  kLimit,
  kPartition,
  kProjection,
  kSort,
  kUnion,
};

/**
 * AbstractOperator is the abstract super class for all query execution operators. Operators have up to two input tables
 * and one output table.
 */
class AbstractOperator : public std::enable_shared_from_this<AbstractOperator>, private Noncopyable {
 public:
  explicit AbstractOperator(OperatorType type, std::shared_ptr<const AbstractOperator> left = nullptr,
                            std::shared_ptr<const AbstractOperator> right = nullptr);

  virtual ~AbstractOperator() = default;

  OperatorType Type() const;

  /**
   * Execute runs the operator's logic and cleanup routine.
   * Execute and GetOutput are split into two methods to allow for easier asynchronous execution.
   */
  void Execute(const std::shared_ptr<OperatorExecutionContext>& operator_execution_context = nullptr);

  /**
   * GetOutput returns the result of the operator that has been executed.
   */
  std::shared_ptr<const Table> GetOutput() const;

  /**
   * Clears the operator's results by releasing the shared pointer to the result table.
   */
  void ClearOutput();

  virtual const std::string& Name() const = 0;
  virtual std::string Description(DescriptionMode description_mode = DescriptionMode::kSingleLine) const;

  /**
   * LeftInput and RightInput return the input operators.
   */
  std::shared_ptr<const AbstractOperator> LeftInput() const;
  std::shared_ptr<const AbstractOperator> RightInput() const;

  /**
   * MutableLeftInput and MutableRightInput return mutable versions of the input operators.
   * These methods cast away the constness from the operators.
   */
  std::shared_ptr<AbstractOperator> MutableLeftInput() const;
  std::shared_ptr<AbstractOperator> MutableRightInput() const;

  /**
   * LeftInputTable and RightInputTable return the output tables of the inputs.
   */
  std::shared_ptr<const Table> LeftInputTable() const;
  std::shared_ptr<const Table> RightInputTable() const;

 protected:
  /**
   * OnExecute implements the operator's logic.
   */
  virtual std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context = nullptr) = 0;

  /**
   * OnCleanup allows operator-specific cleanups for temporary data.
   * It is separate from OnExecute for readability and as a reminder to clean up after execution, if applicable.
   */
  virtual void OnCleanup();

 private:
  const OperatorType type_;

  /**
   * left_input_ and right_input_ hold shared pointers to input operators, can be nullptr.
   */
  std::shared_ptr<const AbstractOperator> left_input_;
  std::shared_ptr<const AbstractOperator> right_input_;

  /**
   * output_ is nullptr until the operator is executed.
   */
  std::shared_ptr<const Table> output_;
};

}  // namespace skyrise
