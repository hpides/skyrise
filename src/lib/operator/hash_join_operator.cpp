#include "hash_join_operator.hpp"

#include <tuple>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace {

static const std::string kName = "HashJoin";

}  // namespace

namespace skyrise {

HashJoinOperator::HashJoinOperator(std::shared_ptr<const AbstractOperator> left_input,
                                   std::shared_ptr<const AbstractOperator> right_input,
                                   std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode)
    : AbstractOperator(OperatorType::kHashJoin, std::move(left_input), std::move(right_input)),
      predicate_(std::move(predicate)),
      join_mode_(join_mode) {
  Assert(predicate_->predicate_condition == PredicateCondition::kEquals, "HashJoinOperator only supports Equi-Joins.");
  Assert(join_mode_ == JoinMode::kInner, "HashJoinOperator only supports Inner Joins.");
}

const std::string& HashJoinOperator::Name() const { return kName; }

std::shared_ptr<const Table> HashJoinOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& /*operator_execution_context*/) {
  Assert(!LeftInputTable()->ColumnIsNullable(predicate_->column_id_left) &&
             !RightInputTable()->ColumnIsNullable(predicate_->column_id_right),
         "HashJoinOperator does not support nullable columns.");
  Assert(LeftInputTable()->ColumnDataType(predicate_->column_id_left) ==
             RightInputTable()->ColumnDataType(predicate_->column_id_right),
         "Left and right join column must have the same type.");

  PositionLists position_lists(RightInputTable()->ChunkCount());

  ResolveDataType(LeftInputTable()->ColumnDataType(predicate_->column_id_left), [&](auto data_type) {
    using ColumnDataType = decltype(data_type);

    // Build.
    std::unordered_multimap<ColumnDataType, RowId> build_table;

    for (ChunkId i = 0; i < LeftInputTable()->ChunkCount(); ++i) {
      const auto input_chunk = LeftInputTable()->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(predicate_->column_id_left);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        build_table.emplace(segment_values[j], RowId{i, j});
      }
    }

    // Probe.
    for (ChunkId i = 0; i < RightInputTable()->ChunkCount(); ++i) {
      const auto input_chunk = RightInputTable()->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(predicate_->column_id_right);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        auto matches = build_table.equal_range(segment_values[j]);

        for (auto it = matches.first; it != matches.second; ++it) {
          position_lists[i].emplace_back(RowId{it->second.chunk_id, it->second.chunk_offset}, j);
        }
      }
    }
  });

  // Materialize.
  const size_t result_column_count = LeftInputTable()->GetColumnCount() + RightInputTable()->GetColumnCount();
  const size_t result_row_count =
      std::accumulate(position_lists.cbegin(), position_lists.cend(), 0,
                      [&](const auto sum, const auto& position_list) { return sum + position_list.size(); });

  Segments output_segments;
  output_segments.reserve(result_column_count);

  // Materialize left side.
  for (ColumnCount i = 0; i < LeftInputTable()->GetColumnCount(); ++i) {
    ResolveDataType(LeftInputTable()->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<std::vector<ColumnDataType>*> input_segments;
      input_segments.reserve(LeftInputTable()->ChunkCount());

      for (ChunkId j = 0; j < LeftInputTable()->ChunkCount(); ++j) {
        const auto abstract_segment = LeftInputTable()->GetChunk(j)->GetSegment(i);
        const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        input_segments.push_back(&typed_segment->Values());
      }

      std::vector<ColumnDataType> output_segment_values;
      output_segment_values.reserve(result_row_count);

      for (const auto& position_list : position_lists) {
        for (const auto& position : position_list) {
          const auto& input_segment = *input_segments[position.first.chunk_id];
          output_segment_values.push_back(input_segment[position.first.chunk_offset]);
        }
      }

      output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(output_segment_values)));
    });
  }

  // Materialize right side.
  for (ColumnCount i = 0; i < RightInputTable()->GetColumnCount(); ++i) {
    ResolveDataType(RightInputTable()->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<ColumnDataType> output_segment_values;
      output_segment_values.reserve(result_row_count);

      for (ChunkId j = 0; j < RightInputTable()->ChunkCount(); ++j) {
        const auto abstract_segment = RightInputTable()->GetChunk(j)->GetSegment(i);
        const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        const auto segment_values = typed_segment->Values();

        for (const auto& position : position_lists[j]) {
          output_segment_values.push_back(segment_values[position.second]);
        }
      }

      output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(output_segment_values)));
    });
  }

  std::vector<std::shared_ptr<Chunk>> output_chunk = {std::make_shared<Chunk>(std::move(output_segments))};
  TableColumnDefinitions definitions =
      Concatenated(LeftInputTable()->ColumnDefinitions(), RightInputTable()->ColumnDefinitions());

  return std::make_shared<Table>(definitions, std::move(output_chunk));
}

}  // namespace skyrise
