#include "hash_join_operator.hpp"

#include <tuple>
#include <unordered_map>

#include "all_type_variant.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace {

const std::string kName = "HashJoin";

}  // namespace

namespace skyrise {

HashJoinOperator::HashJoinOperator(std::shared_ptr<const AbstractOperator> left_input,
                                   std::shared_ptr<const AbstractOperator> right_input,
                                   std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode)
    : AbstractOperator(OperatorType::kHashJoin, std::move(left_input), std::move(right_input)),
      predicate_(std::move(predicate)),
      join_mode_(join_mode) {
  Assert(predicate_->predicate_condition == PredicateCondition::kEquals, "HashJoinOperator only supports Equi-Joins.");
  Assert(join_mode_ == JoinMode::kInner || join_mode_ == JoinMode::kLeftOuter,
         "HashJoinOperator only supports Inner and Left Outer Joins.");
}

const std::string& HashJoinOperator::Name() const { return kName; }

std::shared_ptr<const Table> HashJoinOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& /*operator_execution_context*/) {
  /*
   * RUNNING EXAMPLE: The operator is explained by using the following table schemas and instances as example
   *
   * Let S, R be tables with the following instance:
   *        ----- S -----               ----- R -----
   *        | A | B | C |               | C | D | E |
   *        ...CHUNK 0...               ...CHUNK 0...
   *        | a | 2 | 1 |               | 2 | x | p |
   *        | b | 2 | 2 |   JOIN on C   | 2 | y | q |
   *        ...CHUNK 1...               | 3 | z | f |
   *        | c | 3 | 2 |               ...CHUNK 1...
   *        | d | 0 | 4 |               | 1 | z | l |
   *        | d | 0 | 3 |               | 3 | b | c |
   *        -------------               -------------
   *
   * The table below is the result of a JOIN-Operation on S and R (with C as JOIN-Attribute):
   *
   *  RowId = (ChunkIndex, ChunkOffset)
   *
   *     RowId    ------- S JOIN R --------    RowId
   *              | A | B |S.C|R.C| D | E |
   *     (0,0)    | a | 2 | 1 | 1 | z | l |    (1,0)
   *  #  (0,1)    | b | 2 | 2 | 2 | x | p |    (0,0)
   *     (0,1)    | b | 2 | 2 | 2 | y | q |    (0,1)
   *     (1,0)    | c | 3 | 2 | 2 | x | p |    (0,0)
   *     (1,0)    | c | 3 | 2 | 2 | y | p |    (0,1)
   *     (1,2)    | d | 0 | 3 | 3 | z | f |    (0,2)
   *     (1,2)    | d | 0 | 3 | 3 | b | c |    (1,1)
   *              -------------------------
   */

  Assert(!LeftInputTable()->ColumnIsNullable(predicate_->column_id_left) &&
             !RightInputTable()->ColumnIsNullable(predicate_->column_id_right),
         "HashJoinOperator does not support nullable columns.");
  Assert(LeftInputTable()->ColumnDataType(predicate_->column_id_left) ==
             RightInputTable()->ColumnDataType(predicate_->column_id_right),
         "Left and right join column must have the same type.");

  /*
   * PositionLists is a two-dimensional vector that stores a list of matching tuple-pairs for each chunk of the right
   * table.
   * Hence, the first dimension is formed by the chunks of the right table. PositionLists[0] holds all matches
   * (according to the join predicate) of tuples if the right tuple is stored in the first chunk of its table.
   * A join-match is stated as Pair composed by a RowId and ChunkOffset. The RowId identifies which tuple of the left
   * table is involved while the ChunkOffset indicates the tuple of the right table (as the ChunkId is the key for the
   * PositionList).
   *
   * Each PositionList will have the structure
   *    [ChunkIndex of R] => [(RowId of S, ChunkOffset of R); ...]
   * Specifically the row of S JOIN R marked with the # in the example will produce the following entry:
   *    [0] => [...; ((0,1), 0); ...]
   *
   * Hence, the PositionLists-Object for such a scenario will look like
   *    [0] => [((0,1), 0); ((0,1), 1); ((1,0), 0); ((1,0), 1); ((1,2), 2)]
   *    [1] => [((0,0), 0); ((1,2), 1)]
   */
  PositionLists position_lists(RightInputTable()->ChunkCount());

  /*
   * This bitmap plays a central role for LEFT OUTER JOINs. The two-dimensional-vector is to be filled as follows:
   *
   *              left_table_matched[i][j] := Tuple with RowId (ChunkIndex=i,ChunkOffset=j) of left table
   *                                          is matched with any tuple of the right table
   *
   * Thus, the bitmap states whether a tuple is unmatched and must be considered for that reason
   * in a further step for Left Outer Joins or not.
   */
  std::vector<std::vector<bool>> left_table_matched;
  left_table_matched.reserve(LeftInputTable()->ChunkCount());
  size_t number_of_unmatched_tuples_left = LeftInputTable()->RowCount();

  ResolveDataType(LeftInputTable()->ColumnDataType(predicate_->column_id_left), [&](auto data_type) {
    using ColumnDataType = decltype(data_type);

    /*
     * The Build-Table is central for determining join-matches for a given join-column-value.
     * Currently, the HashJoin only supports simple predicates with equality in one column per table.
     *
     * This data-structure associates every value of the Column that forms the Join-Predicate for the
     * left table with the RowIds of the Rows, where this value is present.
     * Hint: RowId = (ChunkIndex, ChunkOffset)
     *
     * Resulting Build Table for the aforementioned example (see top of method):
     *    1 => [(0,0)]
     *    2 => [(0,1); (1,0)]
     *    3 => [(1,2)]
     *    4 => [(1,1)]
     */
    std::unordered_multimap<ColumnDataType, RowId> build_table;

    for (ChunkId i = 0; i < LeftInputTable()->ChunkCount(); ++i) {
      const auto input_chunk = LeftInputTable()->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(predicate_->column_id_left);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      left_table_matched.emplace_back(input_chunk->Size(), false);

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        build_table.emplace(segment_values[j], RowId{i, j});
      }
    }

    /*
     * In the probe phase, the join matches are identified and thus the position lists are created. This is achieved
     * by iterating over the rows of the right table.
     * For each row r, the following algorithm looks up in the build table which RowIds of the left table are associated
     * with the value of the join column in r.
     *
     * In addition, all rows of the left table are marked as true if they have at least one join match.
     */
    for (ChunkId i = 0; i < RightInputTable()->ChunkCount(); ++i) {
      const auto input_chunk = RightInputTable()->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(predicate_->column_id_right);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        auto matches = build_table.equal_range(segment_values[j]);

        for (auto it = matches.first; it != matches.second; ++it) {
          if (!left_table_matched[it->second.chunk_id][it->second.chunk_offset]) {
            number_of_unmatched_tuples_left--;
            // Mark corresponding tuple of left table as matched.
            left_table_matched[it->second.chunk_id][it->second.chunk_offset] = true;
          }
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

  // Materialize left side for all join-matches.
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

  // Materialize right side for all join-matches.
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

  std::vector<std::shared_ptr<Chunk>> output_chunks = {std::make_shared<Chunk>(std::move(output_segments))};

  auto right_schema = RightInputTable()->ColumnDefinitions();

  if (join_mode_ == JoinMode::kLeftOuter) {
    for (auto& column : right_schema) {
      column.nullable = true;
    }
  }

  const TableColumnDefinitions definitions = Concatenated(LeftInputTable()->ColumnDefinitions(), right_schema);

  // materialize all unmatched tuples of left table for left outer joins
  if (join_mode_ == JoinMode::kLeftOuter) {
    Segments unmatched_segments;
    unmatched_segments.reserve(result_column_count);

    for (ColumnCount i = 0; i < LeftInputTable()->GetColumnCount(); ++i) {
      ResolveDataType(LeftInputTable()->ColumnDataType(i), [&](auto data_type) {
        using ColumnDataType = decltype(data_type);

        std::vector<ColumnDataType> unmatched_segment_values;
        unmatched_segment_values.reserve(number_of_unmatched_tuples_left);

        for (ChunkId j = 0; j < LeftInputTable()->ChunkCount(); ++j) {
          const auto& abstract_segment = LeftInputTable()->GetChunk(j)->GetSegment(i);
          const auto& typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
          const auto& segment_values = typed_segment->Values();

          for (ChunkOffset k = 0; k < LeftInputTable()->GetChunk(j)->Size(); ++k) {
            if (!left_table_matched[j][k]) {
              unmatched_segment_values.push_back(segment_values[k]);
            }
          }
        }

        unmatched_segments.push_back(
            std::make_shared<ValueSegment<ColumnDataType>>(std::move(unmatched_segment_values)));
      });
    }

    for (ColumnCount i = 0; i < RightInputTable()->GetColumnCount(); ++i) {
      ResolveDataType(RightInputTable()->ColumnDataType(i), [&](auto data_type) {
        using ColumnDataType = decltype(data_type);

        unmatched_segments.push_back(
            std::make_shared<ValueSegment<ColumnDataType>>(std::vector<ColumnDataType>(number_of_unmatched_tuples_left),
                                                           std::vector<bool>(number_of_unmatched_tuples_left, true)));
      });
    }

    output_chunks.emplace_back(std::make_shared<Chunk>(std::move(unmatched_segments)));
  }

  return std::make_shared<Table>(definitions, std::move(output_chunks));
}

}  // namespace skyrise
