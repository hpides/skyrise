/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "aggregate_hash_operator.hpp"

#include <cmath>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "aggregate_traits.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/string_agg_aggregate_expression.hpp"
#include "scheduler/worker/generic_task.hpp"
#include "utils/assert.hpp"

namespace {

const std::string kName = "AggregateHash";
constexpr uint64_t kShortStringIdCounter = 5'000'000'000;
constexpr std::string_view kStringAggDefaultDelimiter = ",";
constexpr uint8_t kStringLengthThreshold = 5;

}  // namespace

namespace skyrise {

// If we store the result of the hashmap lookup (i.e., the index into results) in the AggregateKeyEntry, we do this
// by storing the index in the lower 63 bits of first key entry and setting the most significant bit to 1 as a
// marker that the AggregateKeyEntry now contains a cached result. We can do this because AggregateKeyEntry can not
// become larger than the maximum size of a table (i.e., the maximum representable RowId), which is 2^31 * 2^31 ==
// 2^62. This avoids making the AggregateKey bigger. Adding another 64-bit value (for an index of 2^62 values) for
// the cached value would double the size of the AggregateKey in the case of a single GROUP BY column, thus having
// the utilization of the CPU cache. Same for a discriminating UNION, where the data structure alignment would also
// result in another 8 bytes being used.
constexpr uint64_t kCacheMask = AggregateKeyEntry{1} << 63u;

/**
 * GetOrAddResult is called once per row when iterating over a column that is to be aggregated. The row's key has
 * been calculated as part of AggregateHashOperator::PartitionByGroupByKeys. We also pass in the row id of that row.
 * This row id is stored in results so that we can later use it to reconstruct the values in the GROUP BY columns.
 * If the operator calculates multiple aggregate functions, we only need to perform this lookup as part of the first
 * aggregate function. By setting CacheResultIds to true_type, we can store the result of the lookup in the
 * AggregateKey. Following aggregate functions can then retrieve the index from the AggregateKey.
 */
template <typename CacheResultIds, typename ResultIds, typename Results, typename AggregateKey>
typename Results::reference GetOrAddResult(CacheResultIds /*cache_result_ids*/, ResultIds& result_ids, Results& results,
                                           AggregateKey& key, const RowId& row_id) {
  if constexpr (std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    // No GROUP BY columns are defined for this aggregate operator. We still want to keep most code paths similar and
    // avoid special handling. Thus, GetOrAddResult is still called, however, we always return the same result
    // reference.
    if (results.empty()) {
      results.emplace_back();
      results.back().row_id = row_id;
    }
    return results.back();
  } else {
    // As described above, we may store the index into the results vector in the AggregateKey. If the AggregateKey
    // contains multiple entries, we use the first one. As such, we store a (non-owning, raw) pointer to either the only
    // or the first entry in first_key_entry. We need a raw pointer as a reference cannot be null or reset.
    AggregateKeyEntry* first_key_entry = nullptr;
    if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
      first_key_entry = &key;
    } else {
      first_key_entry = &key[0];
    }

    static_assert(std::is_same_v<AggregateKeyEntry, uint64_t>,
                  "Expected AggregateKeyEntry to be unsigned 64-bit value.");

    // Check if the AggregateKey already contains a stored index.
    if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
      if (*first_key_entry & kCacheMask) {
        // The most significant bit is a 1, remove it by XORing the mask gives us the index into the results vector.
        const AggregateKeyEntry result_id = *first_key_entry ^ kCacheMask;

        // If we have not seen this index as part of the current aggregate function, the results vector may not yet have
        // the correct size. Resize it if necessary and write the current row_id so that we can recover the GROUP BY
        // column(s) later. By default, the newly created values have a kNullRowId and are later ignored. We grow
        // the vector slightly more than necessary. Otherwise, monotonically increasing keys would lead to one resize
        // per row.
        if (result_id >= results.size()) {
          results.resize(static_cast<size_t>((result_id + 1) * 1.5));
        }
        results[result_id].row_id = row_id;

        return results[result_id];
      }
    } else {
      Assert(!(*first_key_entry & kCacheMask),
             "CacheResultIds is set to false, but a cached or immediate key shortcut entry was found.");
    }

    // Lookup the key in the result_ids map.
    const auto result_id_iterator = result_ids.find(key);
    if (result_id_iterator != result_ids.end()) {
      // We have already seen this group and need to return a reference to the group's result.
      const auto result_id = result_id_iterator->second;
      if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
        // If requested, store the index of the first_key_entry and set the most significant bit to 1.
        *first_key_entry = kCacheMask | result_id;
      }
      return results[result_id];
    }

    // We are seeing this group (i.e., this AggregateKey) for the first time, so we need to add result_id_iterator to
    // the list of results and set the row_id needed for restoring the GROUP BY column(s).
    const auto result_id = results.size();
    result_ids.emplace_hint(result_id_iterator, key, result_id);

    results.emplace_back();
    results[result_id].row_id = row_id;

    if constexpr (std::is_same_v<CacheResultIds, std::true_type>) {
      // If requested, store the index of the first_key_entry and set the most significant bit to 1.
      *first_key_entry = kCacheMask | result_id;
    }

    return results[result_id];
  }
}

template <typename AggregateKey>
AggregateKey& GetAggregateKey([[maybe_unused]] KeysPerChunk<AggregateKey>& keys_per_chunk,
                              [[maybe_unused]] const ChunkId chunk_id,
                              [[maybe_unused]] const ChunkOffset chunk_offset) {
  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    auto& hash_keys = keys_per_chunk[chunk_id];

    return hash_keys[chunk_offset];
  } else {
    // We have to return a reference to something, so we create a static EmptyAggregateKey here which is used by
    // every call.
    static EmptyAggregateKey empty_aggregate_key;
    return empty_aggregate_key;
  }
}

/**
 * Visitor context for the AggregateVisitor. The AggregateResultContext can be used without knowing the
 * AggregateKey, the AggregateContext is the full version.
 */
template <typename ColumnDataType, AggregateFunction AggregateFunction>
class AggregateResultContext : public SegmentVisitorContext {
 public:
  // In cases where we know how many values to expect, we can preallocate the context in order to avoid later
  // re-allocations.
  explicit AggregateResultContext(const size_t pre_allocated_size = 0) : results_(pre_allocated_size) {}

  // NOLINTNEXTLINE(readability-identifier-naming)
  AggregateResults<ColumnDataType, AggregateFunction> results_;
};

template <typename ColumnDataType, AggregateFunction AggregateFunction, typename AggregateKey>
class AggregateContext : public AggregateResultContext<ColumnDataType, AggregateFunction> {
 public:
  explicit AggregateContext(const size_t pre_allocated_size = 0)
      : AggregateResultContext<ColumnDataType, AggregateFunction>(pre_allocated_size),
        result_ids_(std::make_unique<AggregateResultIdMap<AggregateKey>>()) {}

  // NOLINTNEXTLINE(readability-identifier-naming)
  std::unique_ptr<AggregateResultIdMap<AggregateKey>> result_ids_;
};

AggregateHashOperator::AggregateHashOperator(std::shared_ptr<AbstractOperator> input_operator,
                                             std::vector<std::shared_ptr<AggregateExpression>> aggregates,
                                             std::vector<ColumnId> group_by_column_ids)
    : AbstractAggregateOperator(std::move(input_operator), std::move(aggregates), std::move(group_by_column_ids)) {
  // NOLINTNEXTLINE(cppcoreguidelines-prefer-member-initializer)
  has_aggregate_functions_ =
      !aggregates_.empty() && !std::all_of(aggregates_.begin(), aggregates_.end(), [](const auto aggregate_expression) {
        return aggregate_expression->GetAggregateFunction() == AggregateFunction::kAny;
      });
}

const std::string& AggregateHashOperator::Name() const { return kName; }

void AggregateHashOperator::OnCleanup() { contexts_per_column_.clear(); }

template <typename ColumnDataType, AggregateFunction AggregateFunction, typename AggregateKey>
__attribute__((hot)) void AggregateHashOperator::AggregateSegment(ChunkId chunk_id, ColumnId column_index,
                                                                  const AbstractSegment& abstract_segment,
                                                                  KeysPerChunk<AggregateKey>& keys_per_chunk) {
  using AggregateType = typename AggregateTraits<ColumnDataType, AggregateFunction>::AggregateType;

  auto aggregator = AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction>().GetAggregateFunction();

  auto& context = *std::static_pointer_cast<AggregateContext<ColumnDataType, AggregateFunction, AggregateKey>>(
      contexts_per_column_[column_index]);

  auto& result_ids = *context.result_ids_;
  auto& results = context.results_;

  ChunkOffset chunk_offset = 0;

  // CacheResultIds is a boolean type parameter that is forwarded to GetOrAddResult.
  const auto process_position = [&](const auto cache_result_ids, const AllTypeVariant& value) {
    auto& result = GetOrAddResult(cache_result_ids, result_ids, results,
                                  GetAggregateKey<AggregateKey>(keys_per_chunk, chunk_id, chunk_offset),
                                  RowId{chunk_id, chunk_offset});
    // If the value is NULL, the current aggregate value does not change.
    if (!VariantIsNull(value)) {
      if constexpr (AggregateFunction == AggregateFunction::kCountDistinct) {
        // For the case of COUNT DISTINCT, insert the current value into the set to keep track of distinct values.
        result.accumulator.insert(std::get<ColumnDataType>(value));
      } else {
        aggregator(ColumnDataType(std::get<ColumnDataType>(value)), result.aggregate_count, result.accumulator,
                   RowId{chunk_id, chunk_offset});
      }

      ++result.aggregate_count;
    }

    ++chunk_offset;
  };

  // Pass true_type into GetOrAddResult to enable certain optimizations. If we have more than one aggregate function
  // (and thus more than one context), it makes sense to cache the results indexes.
  // Furthermore, if we use the immediate key shortcut (which uses the same code path as caching), we need to pass
  // true_type so that the aggregate keys are checked for immediate access values.
  if (contexts_per_column_.size() > 1 || use_immediate_key_shortcut_) {
    for (size_t i = 0; i < abstract_segment.Size(); ++i) {
      process_position(std::true_type(), abstract_segment[i]);
    }
  } else {
    for (size_t i = 0; i < abstract_segment.Size(); ++i) {
      process_position(std::false_type(), abstract_segment[i]);
    }
  }
}

/**
 * Partition the input chunks by the given GROUP BY key(s). This is done by creating a vector that contains the
 * AggregateKey for each row. It is gradually built by visitors, one for each group segment.
 */
template <typename AggregateKey>
KeysPerChunk<AggregateKey> AggregateHashOperator::PartitionByGroupByKeys(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  KeysPerChunk<AggregateKey> keys_per_chunk;

  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    const auto input_table = LeftInputTable();
    const ChunkId chunk_count = input_table->ChunkCount();

    // Create the actual data structure.
    keys_per_chunk.reserve(chunk_count);
    for (ChunkId chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table->GetChunk(chunk_id);
      if (!chunk) {
        continue;
      }

      if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntries>) {
        keys_per_chunk.emplace_back(chunk->Size(), AggregateKey(group_by_column_ids_.size()));
      } else {
        keys_per_chunk.emplace_back(chunk->Size(), AggregateKey());
      }
    }

    // We want to fill keys_per_chunk[chunk_id][chunk_offset] with something that uniquely identifies the group into
    // which that position belongs. There are a couple of options here (cf. AggregateHashOperator::OnExecute):
    //
    // 0 GROUP BY columns:   No partitioning needed; we don't reach this point because of the check for
    //                       EmptyAggregateKey above.
    // 1 GROUP BY column:    The AggregateKey is one dimensional, i.e., the same as AggregateKeyEntry.
    // > 1 GROUP BY columns: The AggregateKey is multidimensional. The value in
    //                       keys_per_chunk[chunk_id][chunk_offset] is subscribed with the index of the GROUP BY
    //                       columns (not the same as the GROUP BY column_id).
    //
    // To generate a unique identifier, we create a map from the value found in the respective GROUP BY column to
    // a unique uint64_t. The value 0 is reserved for NULL.
    //
    // This has the cost of a hashmap lookup and potential insert for each row and each GROUP BY column. There are
    // some cases in which we can avoid this. These make use of the fact that we can only have 2^64 - 2*2^32 values
    // in a table (due to kInvalidValueId and kInvalidChunkOffset limiting the range of RowIds).
    //
    // (1) For types smaller than AggregateKeyEntry, such as int32_t, their value range can be immediately mapped into
    //     uint64_t. We cannot do the same for int64_t because we need to account for NULL values.
    // (2) For strings not longer than five characters, there are 1+2^(1*8)+2^(2*8)+2^(3*8)+2^(4*8) potential values.
    //     We can immediately map these into a numerical representation by reinterpreting their byte storage as an
    //     integer. The calculation is described below. Note that this is done on a per-string basis and does not
    //     require all strings in the given column to be that short.

    std::vector<std::shared_ptr<AbstractTask>> jobs;
    jobs.reserve(group_by_column_ids_.size());

    for (size_t group_by_column_index = 0; group_by_column_index < group_by_column_ids_.size();
         ++group_by_column_index) {
      const auto perform_partitioning = [&input_table, group_by_column_index, &keys_per_chunk, chunk_count, this]() {
        const ColumnId group_by_column_id = group_by_column_ids_.at(group_by_column_index);
        const DataType data_type = input_table->ColumnDataType(group_by_column_id);

        ResolveDataType(data_type, [&](auto type) {
          using ColumnDataType = decltype(type);

          if constexpr (std::is_same_v<ColumnDataType, int32_t>) {
            // For values with a smaller type than AggregateKeyEntry, we can use the value itself as an
            // AggregateKeyEntry.

            // Track the minimum and maximum key for the immediate key optimization.
            AggregateKeyEntry min_key = std::numeric_limits<AggregateKeyEntry>::max();
            AggregateKeyEntry max_key = 0;

            for (ChunkId chunk_id = 0; chunk_id < chunk_count; ++chunk_id) {
              const auto input_chunk = input_table->GetChunk(chunk_id);
              const auto abstract_segment = input_chunk->GetSegment(group_by_column_id);
              ChunkOffset chunk_offset = 0;
              std::vector<AggregateKey>& keys = keys_per_chunk[chunk_id];
              for (ChunkOffset i = 0; i < abstract_segment->Size(); ++i) {
                const auto int_to_uint = [](const int32_t value) {
                  // We need to convert a potentially negative int32_t value into the uint64_t space. We do not care
                  // about preserving the value, just its uniqueness. Subtract the minimum value in int32_t (which is
                  // negative itself) to get a positive number.
                  const auto shifted_value = static_cast<int64_t>(value) - std::numeric_limits<int32_t>::min();
                  DebugAssert(shifted_value >= 0, "Type conversion failed.");
                  return static_cast<uint64_t>(shifted_value);
                };

                if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                  // Single GROUP BY column.
                  if (VariantIsNull((*abstract_segment)[i])) {
                    keys[chunk_offset] = 0;
                  } else {
                    const AggregateKeyEntry key = int_to_uint(std::get<ColumnDataType>((*abstract_segment)[i])) + 1;

                    keys[chunk_offset] = key;

                    min_key = std::min(min_key, key);
                    max_key = std::max(max_key, key);
                  }
                } else {
                  // Multiple GROUP BY columns.
                  if (VariantIsNull((*abstract_segment)[i])) {
                    keys[chunk_offset][group_by_column_index] = 0;
                  } else {
                    keys[chunk_offset][group_by_column_index] =
                        int_to_uint(std::get<ColumnDataType>((*abstract_segment)[i])) + 1;
                  }
                }
                ++chunk_offset;
              }
            }

            if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
              // In some cases (e.g., TPC-H Q18), we aggregate with consecutive int32_t values being used as a GROUP
              // BY key. Notably, this is the case when aggregating on the serial primary key of a table without
              // filtering the table before. In these cases, we do not need to perform a full hash-based aggregation,
              // but can use the values as immediate indexes into the list of results. To handle smaller gaps, we
              // include cases up to a certain threshold, but at some point these gaps make the approach less
              // beneficial than a proper hash-based approach. Both min_key and max_key do not correspond to the
              // original int32_t value, but are the result of the int_to_uint transformation. As such, they are
              // guaranteed to be positive. This shortcut only works if we are aggregating with a single GROUP BY
              // column (i.e., when we use AggregateKeyEntry) - otherwise, we cannot establish a 1:1 mapping from
              // keys_per_chunk to the result id.
              if (max_key > 0 &&
                  static_cast<double>(max_key - min_key) < static_cast<double>(input_table->RowCount()) * 1.2) {
                // Include space for MIN, MAX, and NULL.
                expected_result_size_ = static_cast<size_t>(max_key - min_key) + 2;
                use_immediate_key_shortcut_ = true;

                // Rewrite the keys and (1) subtract min so that we can also handle consecutive keys that do not start
                // at 1* and (2) set the first bit which indicates that the key is an immediate index into the result
                // vector (see GetOrAddResult).
                // Note: Because of int_to_uint above, the values do not start at 1, anyway.
                for (ChunkId i = 0; i < chunk_count; ++i) {
                  const ChunkOffset chunk_size = input_table->GetChunk(i)->Size();
                  for (ChunkOffset j = 0; j < chunk_size; ++j) {
                    AggregateKey& key = keys_per_chunk[i][j];
                    if (key == 0) {
                      // Key that denotes NULL, do not rewrite but set the cached flag.
                      key = key | kCacheMask;
                    } else {
                      key = (key - min_key + 1) | kCacheMask;
                    }
                  }
                }
              }
            }
          } else {
            // Store unique IDs for equal values in the GROUP BY column (similar to dictionary encoding).
            // The ID 0 is reserved for NULL values. The combined IDs build an AggregateKey for each row.

            // This time, we have no idea how much space we need, so we take some memory and then rely on the
            // automatic resizing. The size is quite random, but since single memory allocations do not cost too much,
            // we rather allocate a bit too much.
            std::unordered_map<ColumnDataType, AggregateKeyEntry, std::hash<ColumnDataType>, std::equal_to<>> id_map;
            AggregateKeyEntry id_counter = 1;

            if constexpr (std::is_same_v<ColumnDataType, std::string>) {
              // We store strings shorter than five characters without using the id_map. For that, we need to reserve
              // the IDs used for short strings (see below).
              id_counter = kShortStringIdCounter;
            }

            for (ChunkId i = 0; i < chunk_count; ++i) {
              const auto input_chunk = input_table->GetChunk(i);
              if (!input_chunk) {
                continue;
              }

              std::vector<AggregateKey>& keys = keys_per_chunk[i];

              const auto abstract_segment = input_chunk->GetSegment(group_by_column_id);
              ChunkOffset chunk_offset = 0;
              for (ChunkOffset j = 0; j < abstract_segment->Size(); ++j) {
                if (VariantIsNull((*abstract_segment)[j])) {
                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys[chunk_offset] = 0;
                  } else {
                    keys[chunk_offset][group_by_column_index] = 0;
                  }
                } else {
                  // We need to generate an ID that is unique for the value. In some cases, we can use an
                  // optimization, in others, we can't. We need to somehow track whether we have found an ID or not.
                  // For this, we first set ID to its maximum value. If after all branches it is still that max
                  // value, no optimized ID generation was applied, and we need to generate the ID using the
                  // id_map.
                  AggregateKeyEntry partition_id = std::numeric_limits<AggregateKeyEntry>::max();

                  if constexpr (std::is_same_v<ColumnDataType, std::string>) {
                    const std::string string_value = std::get<ColumnDataType>((*abstract_segment)[j]);
                    if (string_value.size() < kStringLengthThreshold) {
                      static_assert(std::is_same_v<AggregateKeyEntry, uint64_t>,
                                    "Calculation only valid for uint64_t.");

                      const auto char_to_uint = [](const char string_value, const uint bits) {
                        // Characters may be signed or unsigned. For the calculation as described below, we need signed
                        // chars.
                        return static_cast<uint64_t>(*reinterpret_cast<const uint8_t*>(&string_value)) << bits;
                      };

                      switch (string_value.size()) {
                          // Optimization for short strings.
                          //
                          //    NULL:              0
                          //    str.length() == 0: 1
                          //    str.length() == 1: 2 + (uint8_t) str            // maximum: 257 (2 + 0xff)
                          //    str.length() == 2: 258 + (uint16_t) str         // maximum: 65'793 (258 + 0xffff)
                          //    str.length() == 3: 65'794 + (uint24_t) str      // maximum: 16'843'009
                          //    str.length() == 4: 16'843'010 + (uint32_t) str  // maximum: 4'311'810'305
                          //    str.length() >= 5: map-based identifiers, starting at 5'000'000'000 for better
                          //    distinction.
                          //
                          // This could be extended to longer strings if the size of the input table (and thus the
                          // maximum number of distinct strings) is taken into account.
                        case 0: {
                          partition_id = 1;
                        } break;

                        case 1: {
                          partition_id = 2 + char_to_uint(string_value[0], 0);
                        } break;

                        case 2: {
                          partition_id = 258 + char_to_uint(string_value[1], 8) + char_to_uint(string_value[0], 0);
                        } break;

                        case 3: {
                          partition_id = 65'794 + char_to_uint(string_value[2], 16) + char_to_uint(string_value[1], 8) +
                                         char_to_uint(string_value[0], 0);
                        } break;

                        case 4: {
                          partition_id = 16'843'010 + char_to_uint(string_value[3], 24) +
                                         char_to_uint(string_value[2], 16) + char_to_uint(string_value[1], 8) +
                                         char_to_uint(string_value[0], 0);
                        } break;
                      }
                    }
                  }

                  if (partition_id == std::numeric_limits<AggregateKeyEntry>::max()) {
                    // Could not take the shortcut above, either because we don't have a string or because it is too
                    // long.
                    auto inserted = id_map.try_emplace(std::get<ColumnDataType>((*abstract_segment)[j]), id_counter);

                    partition_id = inserted.first->second;

                    // Check if the id_map didn't have the value as a key and a new element was inserted.
                    if (inserted.second) {
                      ++id_counter;
                    }
                  }

                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys[chunk_offset] = partition_id;
                  } else {
                    keys[chunk_offset][group_by_column_index] = partition_id;
                  }
                }

                ++chunk_offset;
              }
            }

            // We will see at least id_map.size() different groups. We can use this knowledge to preallocate memory
            // for the results. Estimating the number of groups for multiple GROUP BY columns is somewhat hard, so we
            // simply take the number of groups created by the GROUP BY column with the highest number of distinct
            // values.
            size_t previous_max = expected_result_size_.load();
            while (previous_max < id_map.size()) {
              // The expected_result_size_ needs to be automatically updated as the GROUP BY columns are processed in
              // parallel. How to atomically update a maximum value? Refer to
              // https://stackoverflow.com/a/16190791/2204581
              if (expected_result_size_.compare_exchange_strong(previous_max, id_map.size())) {
                break;
              }
            }
          }
        });
      };
      jobs.push_back(std::make_shared<GenericTask>(perform_partitioning));
    }

    // If jobs are available, let the scheduler take care of their execution and wait until they have all finished.
    if (!jobs.empty()) {
      operator_execution_context->GetScheduler()->ScheduleAndWaitForTasks(jobs);
    }
  }

  return keys_per_chunk;
}

template <typename AggregateKey>
void AggregateHashOperator::Aggregate(const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  const auto input_table = LeftInputTable();

  if constexpr (SKYRISE_DEBUG) {
    for (const auto group_by_column_id : group_by_column_ids_) {
      Assert(group_by_column_id < input_table->GetColumnCount(), "The GROUP BY column index is out of bounds.");
    }
  }

  // Check for invalid aggregates.
  ValidateAggregates();

  // Partitioning step.
  KeysPerChunk<AggregateKey> keys_per_chunk = PartitionByGroupByKeys<AggregateKey>(operator_execution_context);

  // Aggregation step.
  contexts_per_column_ = std::vector<std::shared_ptr<SegmentVisitorContext>>(aggregates_.size());

  if (!has_aggregate_functions_) {
    // Insert a dummy context for the DISTINCT implementation.
    // That way, contexts_per_column_ will always have at least one context with results.
    // This is important later on, when we write the group keys into the table.
    // The template parameters (int32_t, AggregateFunction::kMin) do not matter, as we do not calculate an aggregate
    // anyway.
    auto context =
        std::make_shared<AggregateContext<int32_t, AggregateFunction::kMin, AggregateKey>>(expected_result_size_);

    contexts_per_column_.push_back(context);
  }

  // Create an AggregateContext for each column in the input table that a normal (i.e. non-DISTINCT) aggregate is
  // created on. We do this here, and not in the per-chunk-loop below, because there might be no chunks in the input
  // and WriteAggregateOutputFactory() needs these contexts anyway.
  for (ColumnId i = 0; i < aggregates_.size(); ++i) {
    const auto& aggregate = aggregates_[i];

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    const auto& pqp_column = static_cast<const PqpColumnExpression&>(*aggregate->Argument());
    const ColumnId input_column_id = pqp_column.GetColumnId();

    if (input_column_id == kInvalidColumnId) {
      Assert(aggregate->GetAggregateFunction() == AggregateFunction::kCount,
             "Only COUNT may have an invalid ColumnId.");
      // SELECT COUNT(*) - we know the template arguments, so we don't need a visitor.
      auto context = std::make_shared<AggregateContext<CountColumnType, AggregateFunction::kCount, AggregateKey>>(
          expected_result_size_);

      contexts_per_column_[i] = context;
      continue;
    }
    const DataType data_type = input_table->ColumnDataType(input_column_id);
    contexts_per_column_[i] = CreateAggregateContext<AggregateKey>(data_type, aggregate->GetAggregateFunction());
  }

  // Process chunks and perform aggregations.
  const ChunkId chunk_count = input_table->ChunkCount();
  for (ChunkId i = 0; i < chunk_count; ++i) {
    const auto input_chunk = input_table->GetChunk(i);
    if (!input_chunk) {
      continue;
    }

    // Sometimes, gcc is bad at accessing loop conditions only once, so we cache that here.
    const ChunkOffset input_chunk_size = input_chunk->Size();

    if (!has_aggregate_functions_) {
      // DISTINCT implementation.
      // In Skyrise we handle the SQL keyword DISTINCT by using an aggregate operator with grouping but without
      // aggregate functions. All input columns (either explicitly specified as SELECT DISTINCT a, b, c, OR
      // implicitly as SELECT DISTINCT *, are passed as GROUP BY column ids).
      // As the grouping happens as part of the aggregation but no aggregate function exists, we use
      // the AggregateFunction::kMin as a fake aggregate function whose result will be discarded. From here on, the
      // steps are the same as they are for a regular grouped aggregate.
      auto context =
          std::static_pointer_cast<AggregateContext<DistinctColumnType, AggregateFunction::kMin, AggregateKey>>(
              contexts_per_column_[0]);

      auto& result_ids = *context->result_ids_;
      auto& results = context->results_;

      // A value or a combination of values is added to the list of distinct value(s). This is done by calling
      // GetOrAddResult, which adds the corresponding entry in the list of GROUP BY values.
      if (use_immediate_key_shortcut_) {
        for (ChunkOffset j = 0; j < input_chunk_size; ++j) {
          // We are able to use immediate keys, so pass true_type so that the combined caching/immediate key code path
          // is enabled in GetOrAddResult.
          GetOrAddResult(std::true_type(), result_ids, results, GetAggregateKey<AggregateKey>(keys_per_chunk, i, j),
                         RowId{i, j});
        }
      } else {
        // Same as above, but we do not have immediate keys, so we disable that code path to reduce the complexity of
        // GetAggregateKey.
        for (ChunkOffset j = 0; j < input_chunk_size; ++j) {
          GetOrAddResult(std::false_type(), result_ids, results, GetAggregateKey<AggregateKey>(keys_per_chunk, i, j),
                         RowId{i, j});
        }
      }
    } else {
      ColumnId aggregate_index = 0;
      for (const auto& aggregate : aggregates_) {
        // Special COUNT(*) implementation.
        // COUNT(*) does not have a specific target column, therefore we use the maximum ColumnId.
        // We then go through the keys_per_chunk map and count the occurrences of each GROUP BY key.
        // The results are saved in the regular aggregate_count variable so that we don't need a
        // specific output logic for COUNT(*).

        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& pqp_column = static_cast<const PqpColumnExpression&>(*aggregate->Argument());
        const ColumnId input_column_id = pqp_column.GetColumnId();

        if (input_column_id == kInvalidColumnId) {
          Assert(aggregate->GetAggregateFunction() == AggregateFunction::kCount,
                 "Only COUNT may have an invalid ColumnId.");
          auto context =
              std::static_pointer_cast<AggregateContext<CountColumnType, AggregateFunction::kCount, AggregateKey>>(
                  contexts_per_column_[aggregate_index]);

          auto& result_ids = *context->result_ids_;
          auto& results = context->results_;

          if constexpr (std::is_same_v<AggregateKey, EmptyAggregateKey>) {
            // Not grouped by anything, simply count the number of rows.
            results.resize(1);
            results.front().aggregate_count += input_chunk_size;

            // We need to set any row id because the default value (kNullRowId) would later be skipped. As we are not
            // reconstructing the GROUP BY values later, the exact value of this row id does not matter, as long as it
            // not kNullRowId.
            results.front().row_id = RowId{ChunkId{0}, ChunkOffset{0}};
          } else {
            // Count occurrences for each GROUP BY key. If we have more than one aggregate function (and thus more than
            // one context), it makes sense to cache the results indexes, see GetOrAddResult for details.
            if (contexts_per_column_.size() > 1 || use_immediate_key_shortcut_) {
              for (ChunkOffset j = 0; j < input_chunk_size; ++j) {
                auto& result = GetOrAddResult(std::true_type(), result_ids, results,
                                              GetAggregateKey<AggregateKey>(keys_per_chunk, i, j), RowId{i, j});
                ++result.aggregate_count;
              }
            } else {
              for (ChunkOffset j = 0; j < input_chunk_size; ++j) {
                auto& result = GetOrAddResult(std::false_type(), result_ids, results,
                                              GetAggregateKey<AggregateKey>(keys_per_chunk, i, j), RowId{i, j});
                ++result.aggregate_count;
              }
            }
          }

          ++aggregate_index;
          continue;
        }

        const auto abstract_segment = input_chunk->GetSegment(input_column_id);
        const DataType data_type = input_table->ColumnDataType(input_column_id);

        // Invoke correct aggregator for each segment.
        ResolveDataType(data_type, [&, aggregate](auto type) {
          using ColumnDataType = decltype(type);

          switch (aggregate->GetAggregateFunction()) {
            case AggregateFunction::kMin:
              AggregateSegment<ColumnDataType, AggregateFunction::kMin, AggregateKey>(
                  i, aggregate_index, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::kMax:
              AggregateSegment<ColumnDataType, AggregateFunction::kMax, AggregateKey>(
                  i, aggregate_index, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::kSum:
              AggregateSegment<ColumnDataType, AggregateFunction::kSum, AggregateKey>(
                  i, aggregate_index, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::kAvg:
              AggregateSegment<ColumnDataType, AggregateFunction::kAvg, AggregateKey>(
                  i, aggregate_index, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::kCount:
              AggregateSegment<ColumnDataType, AggregateFunction::kCount, AggregateKey>(
                  i, aggregate_index, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::kCountDistinct:
              AggregateSegment<ColumnDataType, AggregateFunction::kCountDistinct, AggregateKey>(
                  i, aggregate_index, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::kStandardDeviationSample:
              AggregateSegment<ColumnDataType, AggregateFunction::kStandardDeviationSample, AggregateKey>(
                  i, aggregate_index, *abstract_segment, keys_per_chunk);
              break;
            case AggregateFunction::kAny:
              // ANY is a pseudo-function and is handled by WriteGroupByOutput.
              break;
            case AggregateFunction::kStringAgg:
              AggregateSegment<ColumnDataType, AggregateFunction::kStringAgg, AggregateKey>(
                  i, aggregate_index, *abstract_segment, keys_per_chunk);
              break;
          }
        });

        ++aggregate_index;
      }
    }
  }
}

std::shared_ptr<const Table> AggregateHashOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  // We do not want the overhead of a vector with heap storage when we have a limited number of aggregate columns.
  // However, more specializations mean more compile time. We now have specializations for 0, 1, 2, and >2 GROUP BY
  // columns.
  switch (group_by_column_ids_.size()) {
    case 0:
      Aggregate<EmptyAggregateKey>(operator_execution_context);
      break;
    case 1:
      // No need for a complex data structure if we only have one entry.
      Aggregate<AggregateKeyEntry>(operator_execution_context);
      break;
    case 2:
      Aggregate<std::array<AggregateKeyEntry, 2>>(operator_execution_context);
      break;
    default:
      Aggregate<AggregateKeyEntries>(operator_execution_context);
      break;
  }

  const uint64_t number_output_columns = group_by_column_ids_.size() + aggregates_.size();
  output_column_definitions_.resize(number_output_columns);
  output_segments_.resize(number_output_columns);

  // If only GROUP BY columns (including ANY pseudo-aggregates) are written, we need to call WriteGroupByOutput.
  // Example: SELECT c_custkey, c_name FROM customer GROUP BY c_custkey, c_name (same as SELECT DISTINCT), which
  // is rewritten to group only on c_custkey and collect c_name as an ANY pseudo-aggregate.
  // Otherwise, it is called by the first call to WriteAggregateOutputFactory.
  if (!has_aggregate_functions_) {
    auto context = std::static_pointer_cast<AggregateResultContext<DistinctColumnType, AggregateFunction::kMin>>(
        contexts_per_column_[0]);
    auto position_list = RowIdPositionList();
    position_list.reserve(context->results_.size());

    for (const auto& aggregate_result : context->results_) {
      // The kNullRowId (just a marker, not literally NULL) means that this aggregate_result is either a gap (in the
      // case of an unused immediate key), or the aggregate_result of overallocating the aggregate_result vector. As
      // such, it must be skipped.
      if (aggregate_result.row_id.IsNull()) {
        continue;
      }
      position_list.push_back(aggregate_result.row_id);
    }
    WriteGroupByOutput(position_list);
  }

  // Write the aggregated columns to the output_table.
  const auto input_table = LeftInputTable();
  ColumnId aggregate_index = 0;

  for (const auto& aggregate : aggregates_) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    const auto& pqp_column = static_cast<const PqpColumnExpression&>(*aggregate->Argument());
    const ColumnId input_column_id = pqp_column.GetColumnId();

    // Output column for COUNT(*).
    const DataType data_type =
        input_column_id == kInvalidColumnId ? DataType::kLong : input_table->ColumnDataType(input_column_id);

    ResolveDataType(data_type, [&, aggregate_index](auto type) {
      WriteAggregateOutputFactory(type, aggregate_index, aggregate->GetAggregateFunction());
    });

    ++aggregate_index;
  }

  // Write the output_table.
  auto output_table = std::make_shared<Table>(output_column_definitions_);
  if (output_segments_.at(0)->Size() > 0) {
    output_table->AppendChunk(output_segments_);
  }

  return output_table;
}

/**
 * The following template functions write the aggregated values for the different aggregate functions.
 * They are separated and templated to avoid compiler errors for invalid type/function combinations.
 * MIN, MAX, SUM, ANY write the current aggregated value.
 */
template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunc>
std::enable_if_t<AggregateFunc == AggregateFunction::kMin || AggregateFunc == AggregateFunction::kMax ||
                     AggregateFunc == AggregateFunction::kSum || AggregateFunc == AggregateFunction::kAny,
                 void>
WriteAggregateValues(std::vector<AggregateType>& values, std::vector<bool>& null_values,
                     const AggregateResults<ColumnDataType, AggregateFunc>& results,
                     const std::shared_ptr<AggregateExpression>& /*aggregate_function*/,
                     const std::shared_ptr<const Table>& /*input_table*/) {
  values.reserve(results.size());
  null_values.reserve(results.size());

  for (const auto& result : results) {
    // kNullRowId (just a marker, not literally NULL) means that this result is either a gap (in the case of an
    // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
    if (result.row_id.IsNull()) {
      continue;
    }

    if (result.aggregate_count > 0) {
      values.push_back(result.accumulator);
      null_values.push_back(false);
    } else {
      values.emplace_back();
      null_values.push_back(true);
    }
  }
}

/**
 * COUNT writes the aggregate counter.
 */
template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunc>
std::enable_if_t<AggregateFunc == AggregateFunction::kCount, void> WriteAggregateValues(
    std::vector<AggregateType>& values, std::vector<bool>& /*null_values*/,
    const AggregateResults<ColumnDataType, AggregateFunc>& results,
    const std::shared_ptr<AggregateExpression>& /*aggregate_function*/,
    const std::shared_ptr<const Table>& /*input_table*/) {
  values.reserve(results.size());

  for (const auto& result : results) {
    // kNullRowId (just a marker, not literally NULL) means that this result is either a gap (in the case of an
    // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
    if (result.row_id.IsNull()) {
      continue;
    }

    values.push_back(result.aggregate_count);
  }
}

/**
 * COUNT(DISTINCT) writes the number of distinct values.
 */
template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunc>
std::enable_if_t<AggregateFunc == AggregateFunction::kCountDistinct, void> WriteAggregateValues(
    std::vector<AggregateType>& values, std::vector<bool>& /*null_values*/,
    const AggregateResults<ColumnDataType, AggregateFunc>& results,
    const std::shared_ptr<AggregateExpression>& /*aggregate_function*/,
    const std::shared_ptr<const Table>& /*input_table*/) {
  values.reserve(results.size());

  for (const auto& result : results) {
    // kNullRowId (just a marker, not literally NULL) means that this result is either a gap (in the case of an
    // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
    if (result.row_id.IsNull()) {
      continue;
    }

    values.push_back(result.accumulator.size());
  }
}

/**
 * AVG writes the calculated average from current aggregate and the aggregate counter.
 */
template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunc>
std::enable_if_t<AggregateFunc == AggregateFunction::kAvg && std::is_arithmetic_v<AggregateType>, void>
WriteAggregateValues(std::vector<AggregateType>& values, std::vector<bool>& null_values,
                     const AggregateResults<ColumnDataType, AggregateFunc>& results,
                     const std::shared_ptr<AggregateExpression>& /*aggregate_function*/,
                     const std::shared_ptr<const Table>& /*input_table*/) {
  values.reserve(results.size());
  null_values.reserve(results.size());

  for (const auto& result : results) {
    // kNullRowId (just a marker, not literally NULL) means that this result is either a gap (in the case of an
    // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
    if (result.row_id.IsNull()) {
      continue;
    }

    if (result.aggregate_count > 0) {
      values.push_back(result.accumulator / static_cast<AggregateType>(result.aggregate_count));
      null_values.push_back(false);
    } else {
      values.emplace_back();
      null_values.push_back(true);
    }
  }
}

/**
 * AVG is not defined for non-arithmetic types. Avoiding compiler errors.
 */
template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunc>
std::enable_if_t<AggregateFunc == AggregateFunction::kAvg && !std::is_arithmetic_v<AggregateType>, void>
WriteAggregateValues(std::vector<AggregateType>& /*values*/, std::vector<bool>& /*null_values*/,
                     const AggregateResults<ColumnDataType, AggregateFunc>& /*results*/,
                     const std::shared_ptr<AggregateExpression>& /*aggregate_function*/,
                     const std::shared_ptr<const Table>& /*input_table*/) {
  Fail("Invalid aggregate");
}

template <typename SortColumnDataType>
RowIdPositionList& MaterializePositions(RowIdPositionList& previously_sorted_position_list,
                                        std::vector<std::pair<RowId, SortColumnDataType>>& row_id_value_pairs) {
  RowIdPositionList position_list;
  position_list.reserve(row_id_value_pairs.size());
  for (const auto& [row_id, _] : row_id_value_pairs) {
    position_list.push_back(row_id);
  }
  previously_sorted_position_list = position_list;
  return previously_sorted_position_list;
}

template <typename AggregateType, typename SortColumnDataType>
std::vector<std::pair<RowId, SortColumnDataType>> MaterializeSortColumn(
    const std::shared_ptr<const Table>& input_table, std::vector<std::pair<RowId, AggregateType>>& result_values,
    RowIdPositionList& previously_sorted_position_list, const SortColumnDefinition& definition) {
  std::vector<std::pair<RowId, SortColumnDataType>> row_id_value_pairs;
  if (!previously_sorted_position_list.empty()) {
    for (const auto& row_id : previously_sorted_position_list) {
      const ChunkId chunk_id = row_id.chunk_id;
      const ChunkOffset chunk_offset = row_id.chunk_offset;
      const auto value_segment = std::static_pointer_cast<ValueSegment<SortColumnDataType>>(
          input_table->GetChunk(chunk_id)->GetSegment(definition.column_id));
      row_id_value_pairs.emplace_back(RowId{chunk_id, chunk_offset}, value_segment->Get(chunk_offset));
    }
  } else {
    for (const auto& value : result_values) {
      const ChunkId chunk_id = value.first.chunk_id;
      const ChunkOffset chunk_offset = value.first.chunk_offset;
      const auto value_segment = std::static_pointer_cast<ValueSegment<SortColumnDataType>>(
          input_table->GetChunk(chunk_id)->GetSegment(definition.column_id));
      row_id_value_pairs.emplace_back(RowId{chunk_id, chunk_offset}, value_segment->Get(chunk_offset));
    }
  }
  return row_id_value_pairs;
}

template <typename SortColumnDataType>
void Sort(const SortColumnDefinition& definition,
          std::vector<std::pair<RowId, SortColumnDataType>>& row_id_value_pairs) {
  const auto sort = [&](auto comparator) {
    std::stable_sort(row_id_value_pairs.begin(), row_id_value_pairs.end(),
                     [&comparator](std::pair<RowId, SortColumnDataType> a, std::pair<RowId, SortColumnDataType> b) {
                       return comparator(a.second, b.second);
                     });
  };

  if (definition.sort_mode == SortMode::kAscending) {
    sort(std::less<>());
  } else {
    sort(std::greater<>());
  }
}

template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunc>
std::enable_if_t<AggregateFunc == AggregateFunction::kStringAgg, void> WriteAggregateValues(
    std::vector<AggregateType>& values, std::vector<bool>& null_values,
    const AggregateResults<ColumnDataType, AggregateFunc>& results,
    const std::shared_ptr<AggregateExpression>& aggregate_function, const std::shared_ptr<const Table>& input_table) {
  values.reserve(results.size());
  null_values.reserve(results.size());

  for (const auto& result : results) {
    if (result.row_id.IsNull()) {
      continue;
    }

    std::vector<std::pair<RowId, AggregateType>> result_values = std::move(result.accumulator);
    const auto string_agg_expression = std::static_pointer_cast<StringAggAggregateExpression>(aggregate_function);
    if (string_agg_expression->OrderWithinGroup()) {
      // With prior sorting.
      const std::vector<SortColumnDefinition> sort_column_definitions = string_agg_expression->SortColumnDefinitions();
      RowIdPositionList previously_sorted_position_list;
      // NOLINTNEXTLINE(modernize-loop-convert)
      for (auto sort_column_definition = sort_column_definitions.rbegin();
           sort_column_definition != sort_column_definitions.rend(); ++sort_column_definition) {
        const auto data_type = input_table->ColumnDataType(sort_column_definition->column_id);

        ResolveDataType(data_type, [&](auto column_data_type) {
          using SortColumnDataType = decltype(column_data_type);

          auto row_id_value_pairs = MaterializeSortColumn<AggregateType, SortColumnDataType>(
              input_table, result_values, previously_sorted_position_list, *sort_column_definition);

          Sort(*sort_column_definition, row_id_value_pairs);

          previously_sorted_position_list = MaterializePositions(previously_sorted_position_list, row_id_value_pairs);
        });
      }

      std::string concatenated;
      for (const auto& row_id : previously_sorted_position_list) {
        if (!concatenated.empty()) {
          concatenated += kStringAggDefaultDelimiter;
        }
        const ColumnId string_agg_column_id =
            std::static_pointer_cast<PqpColumnExpression>(aggregate_function->Argument())->GetColumnId();
        const auto value_segment = std::static_pointer_cast<ValueSegment<std::string>>(
            input_table->GetChunk(row_id.chunk_id)->GetSegment(string_agg_column_id));
        concatenated += value_segment->Get(row_id.chunk_offset);
      }

      if (result.aggregate_count > 0) {
        values.push_back(concatenated);
        null_values.push_back(false);
      } else {
        values.emplace_back();
        null_values.push_back(true);
      }
    } else {
      // Without prior sorting.
      std::string concatenated;
      for (const auto& value : result_values) {
        if (!concatenated.empty()) {
          concatenated += kStringAggDefaultDelimiter;
        }

        concatenated += value.second;
      }

      if (result.aggregate_count > 0) {
        values.push_back(concatenated);
        null_values.push_back(false);
      } else {
        values.emplace_back();
        null_values.push_back(true);
      }
    }
  }
}

/**
 * STDDEV_SAMP writes the calculated standard deviation from the current aggregate and the aggregate counter.
 */
template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunc>
std::enable_if_t<AggregateFunc == AggregateFunction::kStandardDeviationSample && std::is_arithmetic_v<AggregateType>,
                 void>
WriteAggregateValues(std::vector<AggregateType>& values, std::vector<bool>& null_values,
                     const AggregateResults<ColumnDataType, AggregateFunc>& results,
                     const std::shared_ptr<AggregateExpression>& /*aggregate_function*/,
                     const std::shared_ptr<const Table>& /*input_table*/) {
  values.reserve(results.size());
  null_values.reserve(results.size());

  for (const auto& result : results) {
    // kNullRowId (just a marker, not literally NULL) means that this result is either a gap (in the case of an
    // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
    if (result.row_id.IsNull()) {
      continue;
    }

    if (result.aggregate_count > 1) {
      values.push_back(result.accumulator[3]);
      null_values.push_back(false);
    } else {
      // STDDEV_SAMP is undefined for lists with less than two elements.
      values.emplace_back();
      null_values.push_back(true);
    }
  }
}

/**
 * STDDEV_SAMP is not defined for non-arithmetic types. Avoiding compiler errors.
 */
template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunc>
std::enable_if_t<AggregateFunc == AggregateFunction::kStandardDeviationSample && !std::is_arithmetic_v<AggregateType>,
                 void>
WriteAggregateValues(std::vector<AggregateType>& /*values*/, std::vector<bool>& /*null_values*/,
                     const AggregateResults<ColumnDataType, AggregateFunc>& /*results*/,
                     const std::shared_ptr<AggregateExpression>& /*aggregate_function*/,
                     const std::shared_ptr<const Table>& /*input_table*/) {
  Fail("Invalid aggregate");
}

void AggregateHashOperator::WriteGroupByOutput(RowIdPositionList& position_list) {
  const auto input_table = LeftInputTable();

  std::vector<std::pair<ColumnId, ColumnId>> unaggregated_columns = {};
  {
    ColumnId output_column_id = 0;
    for (const ColumnId& input_column_id : group_by_column_ids_) {
      unaggregated_columns.emplace_back(input_column_id, output_column_id);
      ++output_column_id;
    }
    for (const auto& aggregate : aggregates_) {
      if (aggregate->GetAggregateFunction() == AggregateFunction::kAny) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& pqp_column = static_cast<const PqpColumnExpression&>(*aggregate->Argument());
        const ColumnId input_column_id = pqp_column.GetColumnId();
        unaggregated_columns.emplace_back(input_column_id, output_column_id);
      }
      ++output_column_id;
    }
  }

  // For each GROUP BY column resolve its type, iterate over its values, and add them to a new output value segment.
  for (const auto& unaggregated_column : unaggregated_columns) {
    // Structured bindings do not work with the capture below.
    const ColumnId input_column_id = unaggregated_column.first;
    const ColumnId output_column_id = unaggregated_column.second;

    output_column_definitions_[output_column_id] =
        TableColumnDefinition{input_table->ColumnName(input_column_id), input_table->ColumnDataType(input_column_id),
                              input_table->ColumnIsNullable(input_column_id)};

    ResolveDataType(input_table->ColumnDataType(input_column_id), [&](auto typed_value) {
      using ColumnDataType = decltype(typed_value);

      const bool column_is_nullable = input_table->ColumnIsNullable(input_column_id);

      std::vector<ColumnDataType> values;
      values.reserve(position_list.size());
      std::vector<bool> null_values;
      null_values.reserve(column_is_nullable ? position_list.size() : 0);

      for (const auto& row_id : position_list) {
        const std::shared_ptr<AbstractSegment> segment =
            input_table->GetChunk(row_id.chunk_id)->GetSegment(input_column_id);
        auto value_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(segment);

        std::optional<ColumnDataType> optional_value = value_segment->GetTypedValue(row_id.chunk_offset);

        if (!optional_value) {
          values.emplace_back();
          null_values.push_back(true);
        } else {
          values.push_back(*optional_value);
          null_values.push_back(false);
        }
      }

      std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
      if (column_is_nullable) {
        value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
      } else {
        value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
      }

      output_segments_[output_column_id] = value_segment;
    });
  }
}

template <typename ColumnDataType>
void AggregateHashOperator::WriteAggregateOutputFactory(ColumnDataType /*type*/, ColumnId column_index,
                                                        AggregateFunction aggregate_function) {
  switch (aggregate_function) {
    case AggregateFunction::kMin:
      WriteAggregateOutput<ColumnDataType, AggregateFunction::kMin>(column_index);
      break;
    case AggregateFunction::kMax:
      WriteAggregateOutput<ColumnDataType, AggregateFunction::kMax>(column_index);
      break;
    case AggregateFunction::kSum:
      WriteAggregateOutput<ColumnDataType, AggregateFunction::kSum>(column_index);
      break;
    case AggregateFunction::kAvg:
      WriteAggregateOutput<ColumnDataType, AggregateFunction::kAvg>(column_index);
      break;
    case AggregateFunction::kCount:
      WriteAggregateOutput<ColumnDataType, AggregateFunction::kCount>(column_index);
      break;
    case AggregateFunction::kCountDistinct:
      WriteAggregateOutput<ColumnDataType, AggregateFunction::kCountDistinct>(column_index);
      break;
    case AggregateFunction::kStandardDeviationSample:
      WriteAggregateOutput<ColumnDataType, AggregateFunction::kStandardDeviationSample>(column_index);
      break;
    case AggregateFunction::kAny:
      // Written by WriteGroupByOutput.
      break;
    case AggregateFunction::kStringAgg:
      WriteAggregateOutput<ColumnDataType, AggregateFunction::kStringAgg>(column_index);
      break;
  }
}

template <typename ColumnDataType, AggregateFunction AggregateFunction>
void AggregateHashOperator::WriteAggregateOutput(ColumnId aggregate_index) {
  // Retrieve type information from the aggregation traits.
  typename AggregateTraits<ColumnDataType, AggregateFunction>::AggregateType aggregate_type;
  auto aggregate_data_type = AggregateTraits<ColumnDataType, AggregateFunction>::kAggregateDataType;

  const auto& aggregate = aggregates_[aggregate_index];

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  const auto& pqp_column = static_cast<const PqpColumnExpression&>(*aggregate->Argument());
  const ColumnId input_column_id = pqp_column.GetColumnId();

  if (aggregate_data_type == DataType::kNull) {
    // If not specified, it’s the input column’s type.
    aggregate_data_type = LeftInputTable()->ColumnDataType(input_column_id);
  }

  auto context = std::static_pointer_cast<AggregateResultContext<ColumnDataType, AggregateFunction>>(
      contexts_per_column_[aggregate_index]);

  auto& results = context->results_;

  // Before writing the first aggregate column, write all GROUP BY keys into the respective columns.
  if (aggregate_index == 0) {
    RowIdPositionList position_list{};
    position_list.reserve(results.size());
    for (const auto& result : results) {
      // The kNullRowId (just a marker, not literally NULL) means that this result is either a gap (in the case of an
      // unused immediate key) or the result of overallocating the result vector. As such, it must be skipped.
      if (result.row_id.IsNull()) {
        continue;
      }
      position_list.push_back(result.row_id);
    }
    WriteGroupByOutput(position_list);
  }

  // Write aggregated values into the segment. While WriteAggregateValues could track if an actual NULL value was
  // written or not, we rather make the output types consistent independent of the input types. Not sure what the
  // standard says about this.
  std::vector<decltype(aggregate_type)> values;
  std::vector<bool> null_values;

  constexpr bool kNeedsNull =
      (AggregateFunction != AggregateFunction::kCount && AggregateFunction != AggregateFunction::kCountDistinct);

  WriteAggregateValues<ColumnDataType, decltype(aggregate_type), AggregateFunction>(values, null_values, results,
                                                                                    aggregate, LeftInputTable());

  if (group_by_column_ids_.empty() && values.empty()) {
    // If we did not GROUP BY anything, and we have no results, we need to add NULL for most aggregates and 0 for count.
    values.push_back(decltype(aggregate_type){});
    if (kNeedsNull) {
      null_values.push_back(true);
    }
  }

  DebugAssert(kNeedsNull || null_values.empty(), "WriteAggregateValues unexpectedly wrote NULL values.");
  const auto output_column_id = group_by_column_ids_.size() + aggregate_index;
  output_column_definitions_[output_column_id] =
      TableColumnDefinition{aggregate->AsColumnName(), aggregate_data_type, kNeedsNull};

  auto output_segment = std::shared_ptr<ValueSegment<decltype(aggregate_type)>>();
  if (!kNeedsNull) {
    output_segment = std::make_shared<ValueSegment<decltype(aggregate_type)>>(std::move(values));
  } else {
    output_segment =
        std::make_shared<ValueSegment<decltype(aggregate_type)>>(std::move(values), std::move(null_values));
  }
  output_segments_[output_column_id] = output_segment;
}

template <typename AggregateKey>
std::shared_ptr<SegmentVisitorContext> AggregateHashOperator::CreateAggregateContext(
    const DataType data_type, const AggregateFunction aggregate_function) const {
  std::shared_ptr<SegmentVisitorContext> context;
  ResolveDataType(data_type, [&](auto type) {
    const size_t size = expected_result_size_.load();
    using ColumnDataType = decltype(type);

    switch (aggregate_function) {
      case AggregateFunction::kMin:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::kMin, AggregateKey>>(size);
        break;
      case AggregateFunction::kMax:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::kMax, AggregateKey>>(size);
        break;
      case AggregateFunction::kSum:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::kSum, AggregateKey>>(size);
        break;
      case AggregateFunction::kAvg:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::kAvg, AggregateKey>>(size);
        break;
      case AggregateFunction::kCount:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::kCount, AggregateKey>>(size);
        break;
      case AggregateFunction::kCountDistinct:
        context =
            std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::kCountDistinct, AggregateKey>>(size);
        break;
      case AggregateFunction::kStandardDeviationSample:
        context = std::make_shared<
            AggregateContext<ColumnDataType, AggregateFunction::kStandardDeviationSample, AggregateKey>>(size);
        break;
      case AggregateFunction::kAny:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::kAny, AggregateKey>>(size);
        break;
      case AggregateFunction::kStringAgg:
        context = std::make_shared<AggregateContext<ColumnDataType, AggregateFunction::kStringAgg, AggregateKey>>(size);
    }
  });

  return context;
}

}  // namespace skyrise
