#include <set>
#include <vector>

#include "abstract_operator.hpp"
#include "partitioning_function.hpp"
#include "storage/table/table.hpp"

namespace skyrise {

/*
 * Returns a table containing one chunk per partition. Chunks can be empty if the respective partition is empty.
 *
 * TODO(tobodner): Add support for alternative partitioning functions (e.g., range)
 */
class PartitionOperator : public AbstractOperator {
 public:
  PartitionOperator(std::shared_ptr<AbstractOperator> input,
                    std::shared_ptr<AbstractPartitioningFunction> partitioning_function);

  const std::string& Name() const override;

 private:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) override;

  const std::shared_ptr<AbstractPartitioningFunction> partitioning_function_;
};

}  // namespace skyrise
