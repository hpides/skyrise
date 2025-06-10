#pragma once

#include "abstract_operator.hpp"
#include "join_operator_predicate.hpp"
#include "types.hpp"

namespace skyrise {

class HashJoinOperator : public AbstractOperator {
 public:
  /**
   * PositionLists is a two-dimensional vector that stores a list of matching tuple-pairs for each chunk of the right
   * table. Hence, the first dimension is formed by the chunks of the right table. PositionLists[0] holds all matches
   * (according to the join predicate) of tuples if the right tuple is stored in the first chunk of its table.
   * A join match is stated as Pair composed by a RowId and ChunkOffset. The RowId identifies which tuple of the left
   * table is involved while the ChunkOffset indicates the tuple of the right table (as the ChunkId is the key for the
   * PositionList).
   *
   * Each PositionList has the following structure.
   *    [ChunkIndex of R] => [(RowId of S, ChunkOffset of R); ...]
   * Specifically the row of S and R joined marked with the # in the example will produce the following entry:
   *    [0] => [...; ((0,1), 0); ...]
   *
   * Hence, the PositionLists object in this scenario looks as follows.
   *    [0] => [((0,1), 0); ((0,1), 1); ((1,0), 0); ((1,0), 1); ((1,2), 2)]
   *    [1] => [((0,0), 0); ((1,2), 1)]
   */
  using PositionLists = std::vector<std::vector<std::pair<RowId, ChunkOffset>>>;

  HashJoinOperator(std::shared_ptr<const AbstractOperator> left_input,
                   std::shared_ptr<const AbstractOperator> right_input,
                   std::shared_ptr<JoinOperatorPredicate> predicate, const JoinMode join_mode);

  const std::string& Name() const override;

 private:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) override;

  void InitializeAuxiliaryDataStructures();

  TableColumnDefinitions BuildLeftSchema() const;
  TableColumnDefinitions BuildRightSchema() const;

  ColumnCount ComputeResultColumnCount() const;
  int ComputeResultRowCount() const;
  int DetermineOutputChunkCount() const;

  void FillPositionLists();

  void MaterializeMatches(const size_t result_column_count, const size_t result_row_count,
                          std::vector<std::shared_ptr<Chunk>>& output_chunks);
  void MaterializeLeftSideOfMatchedTuples(const size_t result_row_count, Segments& output_segments);
  void MaterializeRightSideOfMatchedTuples(const size_t result_row_count, Segments& output_segments);

  void MaterializeDanglingTuplesOfLeftTable(const size_t result_column_count,
                                            std::vector<std::shared_ptr<Chunk>>& output_chunks);
  void MaterializeDanglingTuplesOfRightTable(const size_t result_column_count,
                                             std::vector<std::shared_ptr<Chunk>>& output_chunks);
  static void AppendNullFilledSegmentsForRowsOfTable(Segments& segments, const std::shared_ptr<const Table>& table,
                                                     size_t row_count);

  const std::shared_ptr<JoinOperatorPredicate> predicate_;
  const JoinMode join_mode_;

  PositionLists position_lists_;

  /**
   * This bitmap plays a central role for left-outer joins. The two-dimensional vector is filled as follows:
   *
   *              left_table_matched[i][j] := Tuple with RowId (ChunkIndex=i,ChunkOffset=j) of left table
   *                                          is matched with any tuple of the right table
   *
   * Thus, the bitmap states whether a tuple is dangling and must be considered for that reason
   * in a further step for Left Outer Joins or not.
   */
  std::vector<std::vector<bool>> dangling_tuples_left_;
  size_t dangling_tuples_left_count_ = 0;

  /**
   * Auxiliary data structures that are used to identify dangling tuples of the right table in outer joins.
   */
  std::vector<std::vector<ChunkOffset>> dangling_tuples_right_offsets_;
  size_t dangling_tuples_right_count_ = 0;
};

}  // namespace skyrise
