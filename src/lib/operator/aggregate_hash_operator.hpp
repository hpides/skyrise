/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "abstract_aggregate_operator.hpp"
#include "aggregate_traits.hpp"
#include "expression/aggregate_expression.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

/**
 * Empty base class for AggregateResultContext.
 */
class SegmentVisitorContext {};

/**
 * For each group in the output, one AggregateResult is created per aggregate function. If no GROUP BY columns are used,
 * one AggregateResult exists per aggregate function.
 * This result contains:
 * - the current (primary) aggregated value,
 * - the number of rows that were used, which are used for AVERAGE, COUNT, and STDDEV_SAMP,
 * - a RowId, pointing into the input data, for any row that belongs into this group. This is needed to fill the GROUP
 *   BY columns later.
 * Optionally, the result may also contain:
 * - a set of DISTINCT values OR
 * - secondary aggregates, which are currently only used by STDDEV_SAMP.
 */
template <typename ColumnDataType, AggregateFunction AggregateFunction>
struct AggregateResult {
  using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction>::AggregateType;

  using DistinctValues = std::unordered_set<ColumnDataType, std::hash<ColumnDataType>, std::equal_to<>>;

  // Find the correct accumulator type using nested conditionals.
  using AccumulatorType = std::conditional_t<
      // For StandardDeviationSample, use StandardDeviationSampleData as the accumulator.
      AggregateFunction == AggregateFunction::kStandardDeviationSample, StandardDeviationSampleData,
      std::conditional_t<
          AggregateFunction == AggregateFunction::kStringAgg, StringAggAccumulator,
          // For CountDistinct, use DistinctValues, otherwise use AggregateType.
          std::conditional_t<AggregateFunction == AggregateFunction::kCountDistinct, DistinctValues, AggregateType>>>;

  AccumulatorType accumulator = {};
  size_t aggregate_count = 0;

  // As described above, this stores a pointer into the input data that is used to later restore the GROUP BY values.
  // A kNullRowId means that the aggregate result is not (yet) valid and should be skipped when materializing the
  // results. There is no ambiguity with actual NULLs because the aggregate operator is not NULL-producing. As such,
  // we know that each valid GROUP BY-group has at least one valid input RowId.
  RowId row_id = kNullRowId;

  // Note that the size of this struct is a significant performance factor. Be careful when adding fields or
  // changing data types.
};

/**
 * This vector holds the results for every group that was encountered and is indexed by AggregateResultId.
 */
template <typename ColumnDataType, AggregateFunction AggregateFunction>
using AggregateResults = std::vector<AggregateResult<ColumnDataType, AggregateFunction>>;
using AggregateResultId = size_t;

/**
 * The AggregateResultIdMap maps AggregateKeys to their index in the list of aggregate results.
 */
template <typename AggregateKey>
using AggregateResultIdMap =
    std::unordered_map<AggregateKey, AggregateResultId, std::hash<AggregateKey>, std::equal_to<AggregateKey>>;

/**
 * The key type that is used for the aggregation map.
 */
using AggregateKeyEntry = uint64_t;

/**
 * A dummy type used as AggregateKey if no GROUP BY columns are present.
 */
struct EmptyAggregateKey {};

/**
 * Used to store AggregateKeys if more than 2 GROUP BY columns are used. The size is a trade-off between the memory
 * consumption in the AggregateKeys vector (which becomes more expensive to read) and the cost of heap lookups. It could
 * also be, e.g., 5, which would be better for queries with 5 GROUP BY columns but worse for queries with 3 or 4 GROUP
 * BY columns.
 */
using AggregateKeyEntries = std::vector<AggregateKeyEntry>;

/**
 * Conceptually, this is a vector<AggregateKey> that, for each row of the input, holds the AggregateKey. For trivially
 * constructible AggregateKey types, we use an uninitialized_vector, which is cheaper to construct.
 */
template <typename AggregateKey>
using AggregateKeys = std::vector<AggregateKey>;

template <typename AggregateKey>
using KeysPerChunk = std::vector<AggregateKeys<AggregateKey>>;

/**
 * Types that are used for the special COUNT(*) and DISTINCT implementations.
 */
using CountColumnType = int32_t;
using CountAggregateType = int64_t;
using DistinctColumnType = int8_t;
using DistinctAggregateType = int8_t;

class AggregateHashOperator : public AbstractAggregateOperator {
 public:
  AggregateHashOperator(std::shared_ptr<AbstractOperator> input_operator,
                        std::vector<std::shared_ptr<AggregateExpression>> aggregates,
                        std::vector<ColumnId> group_by_column_ids);

  const std::string& Name() const override;

  /**
   * Write the aggregated output for a given aggregate column.
   */
  template <typename ColumnDataType, AggregateFunction AggregateFunction>
  void WriteAggregateOutput(ColumnId aggregate_index);

 protected:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) override;

  template <typename AggregateKey>
  KeysPerChunk<AggregateKey> PartitionByGroupByKeys(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context);

  template <typename AggregateKey>
  void Aggregate(const std::shared_ptr<OperatorExecutionContext>& operator_execution_context);

  void OnCleanup() override;

  template <typename ColumnDataType>
  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  void WriteAggregateOutputFactory(ColumnDataType type, ColumnId column_index, AggregateFunction aggregate_function);

  void WriteGroupByOutput(RowIdPositionList& position_list);

  template <typename ColumnDataType, AggregateFunction AggregateFunction, typename AggregateKey>
  void AggregateSegment(ChunkId chunk_id, ColumnId column_index, const AbstractSegment& abstract_segment,
                        KeysPerChunk<AggregateKey>& keys_per_chunk);

  template <typename AggregateKey>
  std::shared_ptr<SegmentVisitorContext> CreateAggregateContext(DataType data_type,
                                                                AggregateFunction aggregate_function) const;

  std::vector<std::shared_ptr<SegmentVisitorContext>> contexts_per_column_;
  bool has_aggregate_functions_;

  std::atomic_size_t expected_result_size_ = 0;
  bool use_immediate_key_shortcut_ = false;
};

}  // namespace skyrise

namespace std {

template <>
struct hash<skyrise::EmptyAggregateKey> {
  size_t operator()(const skyrise::EmptyAggregateKey& /*key*/) const { return 0; }
};

template <>
struct hash<skyrise::AggregateKeyEntries> {
  size_t operator()(const skyrise::AggregateKeyEntries& key) const { return boost::hash_range(key.begin(), key.end()); }
};

template <size_t N>
struct hash<std::array<skyrise::AggregateKeyEntry, N>> {
  size_t operator()(const std::array<skyrise::AggregateKeyEntry, N>& key) const {
    return boost::hash_range(key.begin(), key.end());
  }
};

}  // namespace std
