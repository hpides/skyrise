#include "hash_join_operator.hpp"

#include <unordered_map>
#include <utility>

#include "all_type_variant.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace {

const std::string kNameInner = "InnerHashJoin";
const std::string kNameLeftOuter = "LeftOuterHashJoin";
const std::string kNameRightOuter = "RightOuterHashJoin";
const std::string kNameFullOuter = "FullOuterHashJoin";

}  // namespace

namespace skyrise {

/**
 * RUNNING EXAMPLE: The operator is explained by using the following table schemas and instances as example
 *
 * Let R, S be tables with the following instance:
 *        ----- R -----               ----- S -----
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
 * The table below is the result of a LEFT-OUTER-JOIN-Operation on S and R (with C as JOIN-Attribute):
 *
 * All matches are contained in one chunk. If JoinMode is LeftOuter, RightOuter or FullOuter up to two chunks are
 * appended. One chunk for all dangling tuples of the left input table and one chunk for the dangling tuples of the
 * right input table.
 *
 *  RowId = (ChunkIndex, ChunkOffset)
 *  _ = NULL
 *
 *     RowId    -- R LEFT OUTER JOIN S --    RowId
 *              | A | B |S.C|R.C| D | E |
 *              .........CHUNK 1.........
 *     (0,0)    | a | 2 | 1 | 1 | z | l |    (1,0)
 *  #  (0,1)    | b | 2 | 2 | 2 | x | p |    (0,0)
 *     (0,1)    | b | 2 | 2 | 2 | y | q |    (0,1)
 *     (1,0)    | c | 3 | 2 | 2 | x | p |    (0,0)
 *     (1,0)    | c | 3 | 2 | 2 | y | p |    (0,1)
 *     (1,2)    | d | 0 | 3 | 3 | z | f |    (0,2)
 *     (1,2)    | d | 0 | 3 | 3 | b | c |    (1,1)
 *              .........CHUNK 2.........
 *     (1,1)    | d | 0 | 4 | _ | _ | _ |
 *              -------------------------
 *
 */
HashJoinOperator::HashJoinOperator(std::shared_ptr<const AbstractOperator> left_input,
                                   std::shared_ptr<const AbstractOperator> right_input,
                                   std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode)
    : AbstractOperator(OperatorType::kHashJoin, std::move(left_input), std::move(right_input)),
      predicate_(std::move(predicate)),
      join_mode_(join_mode) {
  Assert(predicate_->predicate_condition == PredicateCondition::kEquals, "HashJoinOperator only supports Equi-Joins.");
  Assert(join_mode_ == JoinMode::kInner || join_mode_ == JoinMode::kLeftOuter || join_mode_ == JoinMode::kRightOuter ||
             join_mode_ == JoinMode::kFullOuter,
         "HashJoinOperator only supports Inner, LeftOuter, RightOuter, and FullOuter Joins.");
}

const std::string& HashJoinOperator::Name() const {
  switch (join_mode_) {
    case JoinMode::kInner:
      return kNameInner;
    case JoinMode::kLeftOuter:
      return kNameLeftOuter;
    case JoinMode::kRightOuter:
      return kNameRightOuter;
    case JoinMode::kFullOuter:
      return kNameFullOuter;
    default:
      Fail("Unsupported join mode. Implemented join modes are Inner, LeftOuter, RightOuter and FullOuter.");
  }
}

std::shared_ptr<const Table> HashJoinOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& /*operator_execution_context*/) {
  Assert(!LeftInputTable()->ColumnIsNullable(predicate_->column_id_left) &&
             !RightInputTable()->ColumnIsNullable(predicate_->column_id_right),
         "HashJoinOperator does not support nullable columns.");
  Assert(LeftInputTable()->ColumnDataType(predicate_->column_id_left) ==
             RightInputTable()->ColumnDataType(predicate_->column_id_right),
         "Left and right join column must have the same type.");
  InitializeAuxiliaryDataStructures();

  // Data structures for result table.
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(DetermineOutputChunkCount());

  TableColumnDefinitions definitions = Concatenated(BuildLeftSchema(), BuildRightSchema());

  // Build and probe.
  FillPositionLists();

  const size_t result_column_count = ComputeResultColumnCount();
  const size_t result_row_count = ComputeResultRowCount();

  MaterializeMatches(result_column_count, result_row_count, output_chunks);

  // Materialize all dangling tuples of left table for left/full outer joins.
  if (join_mode_ == JoinMode::kLeftOuter || join_mode_ == JoinMode::kFullOuter) {
    MaterializeDanglingTuplesOfLeftTable(result_column_count, output_chunks);
  }

  // Materialize all dangling tuples of right table for right/full outer joins.
  if (join_mode_ == JoinMode::kRightOuter || join_mode_ == JoinMode::kFullOuter) {
    MaterializeDanglingTuplesOfRightTable(result_column_count, output_chunks);
  }

  return std::make_shared<Table>(definitions, std::move(output_chunks));
}

void HashJoinOperator::InitializeAuxiliaryDataStructures() {
  position_lists_ = PositionLists(RightInputTable()->ChunkCount());
  dangling_tuples_left_count_ = LeftInputTable()->RowCount();
  dangling_tuples_left_.reserve(LeftInputTable()->ChunkCount());
  dangling_tuples_right_count_ = 0;
  dangling_tuples_right_offsets_.reserve(RightInputTable()->ChunkCount());
}

void HashJoinOperator::FillPositionLists() {
  // NOLINTNEXTLINE(performance-unnecessary-value-param)
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

      dangling_tuples_left_.emplace_back(input_chunk->Size(), true);

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        build_table.emplace(segment_values[j], RowId{.chunk_id = i, .chunk_offset = j});
      }
    }

    /*
     * In the probe phase, the join matches are identified and thus the position lists are created.
     * This is achieved by iterating over the rows of the right table. For each row r, the following
     * algorithm looks up in the build table which RowIds of the left table are associated with the
     * value of the join column in r.
     *
     * In addition, all rows of the left table are marked as true if they have at least one join
     * match.
     */
    for (ChunkId i = 0; i < RightInputTable()->ChunkCount(); ++i) {
      const auto input_chunk = RightInputTable()->GetChunk(i);
      const auto abstract_segment = input_chunk->GetSegment(predicate_->column_id_right);
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
      const auto segment_values = typed_segment->Values();

      std::vector<ChunkOffset> dangling_offsets_in_segment;

      for (ChunkOffset j = 0; j < segment_values.size(); ++j) {
        auto matches = build_table.equal_range(segment_values[j]);

        if (matches.first == matches.second) {
          // No matches for tuple with RowId (i,j) of the right table were found.
          dangling_offsets_in_segment.emplace_back(j);
          dangling_tuples_right_count_++;
          continue;
        }

        for (auto it = matches.first; it != matches.second; ++it) {
          if (dangling_tuples_left_[it->second.chunk_id][it->second.chunk_offset]) {
            dangling_tuples_left_count_--;
            // Mark corresponding tuple of left table as matched.
            dangling_tuples_left_[it->second.chunk_id][it->second.chunk_offset] = false;
          }
          position_lists_[i].emplace_back(RowId{it->second.chunk_id, it->second.chunk_offset}, j);
        }
      }

      // Add ChunkOffsets to ChunkIndex=i to indicate which tuples had no match.
      dangling_tuples_right_offsets_.emplace_back(dangling_offsets_in_segment);
    }
  });
}

void HashJoinOperator::MaterializeMatches(const size_t result_column_count, const size_t result_row_count,
                                          std::vector<std::shared_ptr<Chunk>>& output_chunks) {
  Assert(dangling_tuples_right_offsets_.size() == RightInputTable()->ChunkCount(),
         "PositionLists must be filled before materialization.");

  Segments output_segments;
  output_segments.reserve(result_column_count);

  MaterializeLeftSideOfMatchedTuples(result_row_count, output_segments);
  MaterializeRightSideOfMatchedTuples(result_row_count, output_segments);

  output_chunks.emplace_back(std::make_shared<Chunk>(std::move(output_segments)));
}

void HashJoinOperator::MaterializeLeftSideOfMatchedTuples(const size_t result_row_count, Segments& output_segments) {
  for (ColumnCount i = 0; i < LeftInputTable()->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
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

      for (const auto& position_list : position_lists_) {
        for (const auto& position : position_list) {
          const auto& input_segment = *input_segments[position.first.chunk_id];
          output_segment_values.push_back(input_segment[position.first.chunk_offset]);
        }
      }

      output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(output_segment_values)));
    });
  }
}

void HashJoinOperator::MaterializeRightSideOfMatchedTuples(const size_t result_row_count, Segments& output_segments) {
  for (ColumnCount i = 0; i < RightInputTable()->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(RightInputTable()->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<ColumnDataType> output_segment_values;
      output_segment_values.reserve(result_row_count);

      for (ChunkId j = 0; j < RightInputTable()->ChunkCount(); ++j) {
        const auto abstract_segment = RightInputTable()->GetChunk(j)->GetSegment(i);
        const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        const auto segment_values = typed_segment->Values();

        for (const auto& position : position_lists_[j]) {
          output_segment_values.push_back(segment_values[position.second]);
        }
      }

      output_segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(output_segment_values)));
    });
  }
}

void HashJoinOperator::MaterializeDanglingTuplesOfLeftTable(const size_t result_column_count,
                                                            std::vector<std::shared_ptr<Chunk>>& output_chunks) {
  Segments segments;
  segments.reserve(result_column_count);

  for (ColumnCount i = 0; i < LeftInputTable()->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(LeftInputTable()->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<ColumnDataType> dangling_segment_values;
      dangling_segment_values.reserve(dangling_tuples_left_count_);

      for (ChunkId j = 0; j < LeftInputTable()->ChunkCount(); ++j) {
        const auto& abstract_segment = LeftInputTable()->GetChunk(j)->GetSegment(i);
        const auto& typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        const auto& segment_values = typed_segment->Values();

        for (ChunkOffset k = 0; k < LeftInputTable()->GetChunk(j)->Size(); ++k) {
          if (dangling_tuples_left_[j][k]) {
            dangling_segment_values.push_back(segment_values[k]);
          }
        }
      }

      segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(dangling_segment_values)));
    });
  }

  AppendNullFilledSegmentsForRowsOfTable(segments, RightInputTable(), dangling_tuples_left_count_);

  output_chunks.emplace_back(std::make_shared<Chunk>(std::move(segments)));
}

void HashJoinOperator::MaterializeDanglingTuplesOfRightTable(const size_t result_column_count,
                                                             std::vector<std::shared_ptr<Chunk>>& output_chunks) {
  Segments segments;
  segments.reserve(result_column_count);

  AppendNullFilledSegmentsForRowsOfTable(segments, LeftInputTable(), dangling_tuples_right_count_);

  for (ColumnCount i = 0; i < RightInputTable()->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(RightInputTable()->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);

      std::vector<ColumnDataType> dangling_segment_values;

      for (ChunkId k = 0; k < RightInputTable()->ChunkCount(); ++k) {
        const auto& abstract_segment = RightInputTable()->GetChunk(k)->GetSegment(i);
        const auto& typed_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(abstract_segment);
        const auto& segment_values = typed_segment->Values();

        for (ColumnId& offset : dangling_tuples_right_offsets_[k]) {
          dangling_segment_values.push_back(segment_values[offset]);
        }
      }

      segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(dangling_segment_values)));
    });
  }

  output_chunks.emplace_back(std::make_shared<Chunk>(std::move(segments)));
}

void HashJoinOperator::AppendNullFilledSegmentsForRowsOfTable(Segments& segments,
                                                              const std::shared_ptr<const Table>& table,
                                                              size_t row_count) {
  for (ColumnCount i = 0; i < table->GetColumnCount(); ++i) {
    // NOLINTNEXTLINE(performance-unnecessary-value-param)
    ResolveDataType(table->ColumnDataType(i), [&](auto data_type) {
      using ColumnDataType = decltype(data_type);
      segments.push_back(std::make_shared<ValueSegment<ColumnDataType>>(std::vector<ColumnDataType>(row_count),
                                                                        std::vector<bool>(row_count, true)));
    });
  }
}

int HashJoinOperator::DetermineOutputChunkCount() const {
  switch (join_mode_) {
    case JoinMode::kInner:
      return 1;
    case JoinMode::kLeftOuter:
    case JoinMode::kRightOuter:
      return 2;
    case JoinMode::kFullOuter:
      return 3;
    default:
      Fail("Unsupported join mode. Implemented join modes are Inner, LeftOuter, RightOuter and FullOuter.");
  }
}

int HashJoinOperator::ComputeResultRowCount() const {
  return std::accumulate(position_lists_.cbegin(), position_lists_.cend(), 0,
                         [&](const auto sum, const auto& position_list) { return sum + position_list.size(); });
}

ColumnCount HashJoinOperator::ComputeResultColumnCount() const {
  return LeftInputTable()->GetColumnCount() + RightInputTable()->GetColumnCount();
}

TableColumnDefinitions HashJoinOperator::BuildRightSchema() const {
  auto right_schema = RightInputTable()->ColumnDefinitions();
  if (join_mode_ == JoinMode::kLeftOuter || join_mode_ == JoinMode::kFullOuter) {
    for (auto& column : right_schema) {
      column.nullable = true;
    }
  }
  return right_schema;
}

TableColumnDefinitions HashJoinOperator::BuildLeftSchema() const {
  auto left_schema = LeftInputTable()->ColumnDefinitions();
  if (join_mode_ == JoinMode::kRightOuter || join_mode_ == JoinMode::kFullOuter) {
    for (auto& column : left_schema) {
      column.nullable = true;
    }
  }
  return left_schema;
}

}  // namespace skyrise
