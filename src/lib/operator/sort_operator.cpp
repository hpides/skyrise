#include "sort_operator.hpp"

#include <cmath>

#include "scheduler/worker/generic_task.hpp"
#include "storage/storage_types.hpp"
#include "storage/table/value_segment.hpp"

namespace {

const std::string kName = "Sort";

}  // namespace

namespace skyrise {

SortOperator::SortOperator(std::shared_ptr<const AbstractOperator> input_operator,
                           const std::vector<SortColumnDefinition>& sort_definitions)
    : AbstractOperator(OperatorType::kSort, std::move(input_operator)), sort_definitions_(sort_definitions) {
  DebugAssert(!sort_definitions_.empty(), "Expected at least one sort criterion.");
}

const std::vector<SortColumnDefinition>& SortOperator::SortDefinitions() const { return sort_definitions_; }

const std::string& SortOperator::Name() const { return kName; }

std::shared_ptr<const Table> SortOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  const auto& input_table = LeftInputTable();

  for (const auto& sort_definition : sort_definitions_) {
    Assert(sort_definition.column_id != kInvalidColumnId, "Invalid column in sort definition.");
    Assert(sort_definition.column_id < input_table->GetColumnCount(), "ColumnId is greater than the column count.");
  }

  if (input_table->RowCount() == 0) {
    return input_table;
  }

  // After the first (i.e., least significant) sort operation has been completed, this holds the order of the table as
  // it has been determined so far.
  RowIdPositionList previously_sorted_position_list;
  // NOLINTNEXTLINE(modernize-loop-convert)
  for (auto it = sort_definitions_.rbegin(); it != sort_definitions_.rend(); ++it) {
    const auto data_type = input_table->ColumnDataType(it->column_id);

    ResolveDataType(data_type, [&](auto type) {
      using ColumnDataType = decltype(type);

      SortImplementation<ColumnDataType> sort_implementation(input_table, it->column_id, it->sort_mode);
      previously_sorted_position_list = sort_implementation.Sort(previously_sorted_position_list);
    });
  }

  return MaterializeOutputTable(input_table, previously_sorted_position_list,
                                operator_execution_context->GetScheduler());
}

std::shared_ptr<Table> SortOperator::MaterializeOutputTable(
    const std::shared_ptr<const Table>& unsorted_table, const RowIdPositionList& position_list,
    const std::shared_ptr<FragmentScheduler>& fragment_scheduler) {
  Assert(position_list.size() == unsorted_table->RowCount(),
         "The input table size does not match the number of row-positions.");

  const size_t row_count = unsorted_table->RowCount();
  const size_t number_of_output_chunks = row_count / kChunkDefaultSize + (row_count % kChunkDefaultSize == 0 ? 0 : 1);

  std::vector<std::shared_ptr<AbstractTask>> jobs;
  jobs.reserve(number_of_output_chunks);
  std::vector<std::shared_ptr<Chunk>> chunks(number_of_output_chunks);

  size_t chunk_index = 0;
  for (size_t start_index = 0; start_index < row_count; start_index += kChunkDefaultSize) {
    const size_t end_index = std::min(start_index + kChunkDefaultSize, row_count);

    const auto materialize_chunk_job = [&, start_index, end_index, chunk_index]() {
      Segments output_segments;
      output_segments.reserve(unsorted_table->GetColumnCount());

      for (ColumnId column_id = 0; column_id < unsorted_table->GetColumnCount(); ++column_id) {
        const DataType column_data_type = unsorted_table->ColumnDataType(column_id);
        const bool column_is_nullable = unsorted_table->ColumnIsNullable(column_id);

        ResolveDataType(column_data_type, [&](auto type) {
          using ColumnDataType = decltype(type);

          std::vector<ColumnDataType> value_segment_value_vector;
          value_segment_value_vector.reserve(row_count);
          std::vector<bool> value_segment_null_vector;
          value_segment_null_vector.reserve(row_count);
          for (size_t i = start_index; i < end_index; ++i) {
            const auto& [chunk_id, chunk_offset] = position_list[i];
            const AllTypeVariant& value = (*unsorted_table->GetChunk(chunk_id)->GetSegment(column_id))[chunk_offset];
            const bool value_is_null = VariantIsNull(value);

            value_segment_value_vector.push_back(std::get<ColumnDataType>(value));
            value_segment_null_vector.push_back(value_is_null);
          }

          const std::shared_ptr<ValueSegment<ColumnDataType>> value_segment =
              column_is_nullable
                  ? std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                   std::move(value_segment_null_vector))
                  : std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
          output_segments.push_back(std::move(value_segment));
        });
      };

      chunks[chunk_index] = std::make_shared<Chunk>(std::move(output_segments));
    };

    jobs.push_back(std::make_shared<GenericTask>(std::move(materialize_chunk_job)));
    ++chunk_index;
  }

  fragment_scheduler->ScheduleAndWaitForTasks(jobs);

  return std::make_shared<Table>(unsorted_table->ColumnDefinitions(), std::move(chunks));
}

template <typename SortColumnDataType>
class SortOperator::SortImplementation {
 public:
  using RowIdValuePair = std::pair<RowId, SortColumnDataType>;

  SortImplementation(std::shared_ptr<const Table> input_table, const ColumnId column_id,
                     const SortMode sort_mode = SortMode::kAscending)
      : input_table_(std::move(input_table)), column_id_(column_id), sort_mode_(sort_mode) {
    const size_t row_count = input_table_->RowCount();
    row_id_value_pairs_.reserve(row_count);
    null_value_rows_.reserve(row_count);
  }

  // Sorts input_table_, potentially taking the pre-existing order of previously_sorted_position_list into account.
  // Returns a RowIdPositionList, which can be used for either an input to the next call of sort or for materializing
  // the output table.
  RowIdPositionList Sort(const RowIdPositionList& previously_sorted_position_list) {
    // (1) Prepare Sort: Creating RowId-Value-Structure.
    MaterializeSortColumn(previously_sorted_position_list);

    // (2) After we got our ValueRowId Map we sort the map by the value of the pair.
    const auto sort_with_comparator = [&](auto comparator) {
      std::stable_sort(row_id_value_pairs_.begin(), row_id_value_pairs_.end(),
                       [&comparator](RowIdValuePair a, RowIdValuePair b) { return comparator(a.second, b.second); });
    };
    if (sort_mode_ == SortMode::kAscending) {
      sort_with_comparator(std::less<>());
    } else {
      sort_with_comparator(std::greater<>());
    }

    // (3) Insert null rows in front of all non-NULL rows.
    if (!null_value_rows_.empty()) {
      row_id_value_pairs_.insert(row_id_value_pairs_.begin(), null_value_rows_.begin(), null_value_rows_.end());
    }

    RowIdPositionList position_list;
    position_list.reserve(row_id_value_pairs_.size());
    for (const auto& [row_id, _] : row_id_value_pairs_) {
      position_list.push_back(row_id);
    }

    return position_list;
  }

 protected:
  /**
   * Completely materializes the sort column to create a vector of RowId-Value pairs.
   */
  void MaterializeSortColumn(const RowIdPositionList& previously_sorted_position_list) {
    // If there was no RowIdPositionList passed, this is the first sorting run, and we simply fill our values and nulls
    // data structures from our input table. Otherwise, we will materialize according to the PosList which is the result
    // of the last run.
    if (!previously_sorted_position_list.empty()) {
      MaterializeColumnFromPositionList(previously_sorted_position_list);
    } else {
      for (ChunkId chunk_id = 0; chunk_id < input_table_->ChunkCount(); ++chunk_id) {
        const std::shared_ptr<AbstractSegment> abstract_segment =
            input_table_->GetChunk(chunk_id)->GetSegment(column_id_);

        for (ChunkOffset chunk_offset = 0; chunk_offset < abstract_segment->Size(); ++chunk_offset) {
          const AllTypeVariant value = (*abstract_segment)[chunk_offset];

          if (VariantIsNull(value)) {
            null_value_rows_.emplace_back(RowId{chunk_id, chunk_offset}, SortColumnDataType());
          } else {
            row_id_value_pairs_.emplace_back(RowId{chunk_id, chunk_offset}, std::get<SortColumnDataType>(value));
          }
        }
      }
    }
  }

  void MaterializeColumnFromPositionList(const RowIdPositionList& position_list) {
    for (const auto& row_id : position_list) {
      const auto [chunk_id, chunk_offset] = row_id;

      const std::shared_ptr<AbstractSegment> segment = input_table_->GetChunk(chunk_id)->GetSegment(column_id_);
      const AllTypeVariant value = (*segment)[chunk_offset];

      if (VariantIsNull(value)) {
        null_value_rows_.emplace_back(row_id, SortColumnDataType());
      } else {
        row_id_value_pairs_.emplace_back(row_id, std::get<SortColumnDataType>(value));
      }
    }
  }

  const std::shared_ptr<const Table> input_table_;

  // Column to sort by.
  const ColumnId column_id_;
  const SortMode sort_mode_;

  std::vector<RowIdValuePair> row_id_value_pairs_;

  // Stored as RowIdValuePair for better type compatibility with row_id_value_pairs_ even if the null value is unused.
  std::vector<RowIdValuePair> null_value_rows_;
};

}  // namespace skyrise
