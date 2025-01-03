#include "operator/hash_join_operator.hpp"

#include <gtest/gtest.h>

#include "operator/table_wrapper.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class HashJoinOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::vector<int> values_int_left = {0, 1, 2, 3, 4, 5};
    std::vector<std::string> values_string_left = {"a", "b", "c", "d", "e", "f"};

    const auto value_segment_int_left = std::make_shared<ValueSegment<int>>(std::move(values_int_left));
    const auto value_segment_string_left = std::make_shared<ValueSegment<std::string>>(std::move(values_string_left));

    std::vector<std::shared_ptr<Chunk>> chunk_left = {
        std::make_shared<Chunk>(Segments({value_segment_int_left, value_segment_string_left}))};

    const TableColumnDefinitions definitions_left = {TableColumnDefinition("a", DataType::kInt, false),
                                                     TableColumnDefinition("b", DataType::kString, false)};
    table_left_ = std::make_shared<Table>(definitions_left, std::move(chunk_left));

    std::vector<std::string> values_string_right = {"g", "h", "i", "j", "k", "l", "m", "n", "o"};
    std::vector<int> values_int_right = {9, 0, 8, 1, 7, 3, 6, 4, 5};

    const auto value_segment_string_right = std::make_shared<ValueSegment<std::string>>(std::move(values_string_right));
    const auto value_segment_int_right = std::make_shared<ValueSegment<int>>(std::move(values_int_right));

    std::vector<std::shared_ptr<Chunk>> chunk_right = {
        std::make_shared<Chunk>(Segments({value_segment_string_right, value_segment_int_right}))};

    const TableColumnDefinitions definitions_right = {TableColumnDefinition("c", DataType::kString, false),
                                                      TableColumnDefinition("d", DataType::kInt, false)};
    table_right_ = std::make_shared<Table>(definitions_right, std::move(chunk_right));
  }

  std::shared_ptr<Table> table_left_;
  std::shared_ptr<Table> table_right_;
};

TEST_F(HashJoinOperatorTest, InnerJoin) {
  const auto table_wrapper_left = std::make_shared<TableWrapper>(table_left_);
  const auto table_wrapper_right = std::make_shared<TableWrapper>(table_right_);

  const auto predicate =
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{0, 1, PredicateCondition::kEquals});
  const auto join_operator =
      std::make_shared<HashJoinOperator>(table_wrapper_left, table_wrapper_right, predicate, JoinMode::kInner);

  EXPECT_EQ(join_operator->Name(), "InnerHashJoin");

  table_wrapper_left->Execute();
  table_wrapper_right->Execute();
  join_operator->Execute();

  const auto result = join_operator->GetOutput();

  EXPECT_EQ(result->ChunkCount(), 1);
  EXPECT_EQ(result->GetColumnCount(), 4);
  EXPECT_EQ(result->RowCount(), 5);
}

TEST_F(HashJoinOperatorTest, LeftOuterJoin) {
  const auto table_wrapper_left = std::make_shared<TableWrapper>(table_left_);
  const auto table_wrapper_right = std::make_shared<TableWrapper>(table_right_);

  const auto predicate =
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{0, 1, PredicateCondition::kEquals});
  const auto join_operator =
      std::make_shared<HashJoinOperator>(table_wrapper_left, table_wrapper_right, predicate, JoinMode::kLeftOuter);

  EXPECT_EQ(join_operator->Name(), "LeftOuterHashJoin");

  table_wrapper_left->Execute();
  table_wrapper_right->Execute();
  join_operator->Execute();

  const auto result = join_operator->GetOutput();

  EXPECT_EQ(result->ChunkCount(), 2);
  EXPECT_EQ(result->GetColumnCount(), 4);
  EXPECT_EQ(result->RowCount(), 6);

  for (ChunkId i = 0; i < result->ChunkCount(); ++i) {
    const auto& abstract_segment_left = result->GetChunk(i)->GetSegment(0);
    const auto& values_int_left = std::dynamic_pointer_cast<ValueSegment<int>>(abstract_segment_left)->Values();
    const auto& typed_segment_right = std::dynamic_pointer_cast<ValueSegment<int>>(result->GetChunk(i)->GetSegment(3));

    for (ChunkOffset j = 0; j < result->GetChunk(i)->Size(); ++j) {
      EXPECT_EQ(typed_segment_right->IsNull(j), values_int_left[j] == 2);
    }
  }
}

TEST_F(HashJoinOperatorTest, RightOuterJoin) {
  const auto table_wrapper_left = std::make_shared<TableWrapper>(table_left_);
  const auto table_wrapper_right = std::make_shared<TableWrapper>(table_right_);

  const auto predicate =
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{0, 1, PredicateCondition::kEquals});
  const auto join_operator =
      std::make_shared<HashJoinOperator>(table_wrapper_left, table_wrapper_right, predicate, JoinMode::kRightOuter);

  EXPECT_EQ(join_operator->Name(), "RightOuterHashJoin");

  table_wrapper_left->Execute();
  table_wrapper_right->Execute();
  join_operator->Execute();

  const auto result = join_operator->GetOutput();

  EXPECT_EQ(result->ChunkCount(), 2);
  EXPECT_EQ(result->GetColumnCount(), 4);
  EXPECT_EQ(result->RowCount(), 9);
}

TEST_F(HashJoinOperatorTest, FullOuterJoin) {
  const auto table_wrapper_left = std::make_shared<TableWrapper>(table_left_);
  const auto table_wrapper_right = std::make_shared<TableWrapper>(table_right_);

  const auto predicate =
      std::make_shared<JoinOperatorPredicate>(JoinOperatorPredicate{0, 1, PredicateCondition::kEquals});
  const auto join_operator =
      std::make_shared<HashJoinOperator>(table_wrapper_left, table_wrapper_right, predicate, JoinMode::kFullOuter);

  EXPECT_EQ(join_operator->Name(), "FullOuterHashJoin");

  table_wrapper_left->Execute();
  table_wrapper_right->Execute();
  join_operator->Execute();

  const auto result = join_operator->GetOutput();

  EXPECT_EQ(result->ChunkCount(), 3);
  EXPECT_EQ(result->GetColumnCount(), 4);
  EXPECT_EQ(result->RowCount(), 10);
}

}  // namespace skyrise
