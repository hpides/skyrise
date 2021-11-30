#include <set>
#include <vector>

#include "operator/abstract_operator.hpp"
#include "storage/table/table.hpp"

namespace skyrise {

using PartitionedPositionLists = std::vector<std::vector<std::tuple<ChunkId, size_t>>>;

/*
 * Returns a table containing one chunk per partition. Chunks can be empty if the respective partition is empty.
 *
 * TODO(d-justen): Add support for alternative partitioning functions (e.g., range)
 */
class PartitionOperator : public AbstractOperator {
 public:
  PartitionOperator(std::shared_ptr<AbstractOperator> input, const size_t partition_count,
                    const std::set<ColumnId>& partition_column_ids);

  const std::string& Name() const override;

 private:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context = nullptr) override;
  PartitionedPositionLists GeneratePartitionedPositionLists() const;

  const size_t partition_count_;
  const std::set<ColumnId> partition_column_ids_;
};

}  // namespace skyrise
