#include "operator/sort_operator.hpp"

#include <random>

#include <gtest/gtest.h>

#include "operator/table_wrapper.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class SortOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::vector<int> values_int = {0, 1, 2, 3, 3};
    std::vector<double> values_double = {3, 2, 0, 3, 1};
    std::vector<std::string> values_string = {"2", "0", "1", "3", "3"};

    const auto value_segment_int = std::make_shared<ValueSegment<int>>(std::move(values_int));
    const auto value_segment_double = std::make_shared<ValueSegment<double>>(std::move(values_double));
    const auto value_segment_string = std::make_shared<ValueSegment<std::string>>(std::move(values_string));

    std::vector<std::shared_ptr<Chunk>> chunks = {
        std::make_shared<Chunk>(Segments({value_segment_int, value_segment_double, value_segment_string}))};

    const TableColumnDefinitions definitions = {TableColumnDefinition("a", DataType::kInt, false),
                                                TableColumnDefinition("b", DataType::kDouble, false),
                                                TableColumnDefinition("c", DataType::kString, false)};
    table_ = std::make_shared<Table>(definitions, std::move(chunks));

    operator_execution_context_ = std::make_shared<OperatorExecutionContext>(
        nullptr, nullptr, []() { return std::make_shared<FragmentScheduler>(); });
  }

  template <typename T>
  void SegmentIsEqual(std::shared_ptr<AbstractSegment> segment, std::vector<T> expected_values) {
    const auto expected_segment = std::make_shared<ValueSegment<T>>(std::move(expected_values));

    EXPECT_EQ(segment->GetDataType(), expected_segment->GetDataType());
    EXPECT_EQ(segment->Size(), expected_segment->Size());

    const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);

    for (size_t row_id = 0; row_id < segment->Size(); ++row_id) {
      EXPECT_EQ(value_segment->Values()[row_id], expected_segment->Values()[row_id]);
    }
  }

  std::shared_ptr<const skyrise::Table> GetSortedTable(std::vector<SortColumnDefinition>&& sort_definition) {
    auto mock_input_operator = std::make_shared<TableWrapper>(table_);
    mock_input_operator->Execute();

    const auto sort_operator = std::make_shared<SortOperator>(mock_input_operator, std::move(sort_definition));
    sort_operator->Execute(operator_execution_context_);

    return sort_operator->GetOutput();
  }

  std::shared_ptr<Table> table_;
  std::shared_ptr<OperatorExecutionContext> operator_execution_context_;
};

TEST_F(SortOperatorTest, SortBySingleColumnAscending) {
  const auto chunk = GetSortedTable({SortColumnDefinition(1, SortMode::kAscending)})->GetChunk(0);

  SegmentIsEqual<int>(chunk->GetSegment(0), {2, 3, 1, 0, 3});
  SegmentIsEqual<double>(chunk->GetSegment(1), {0, 1, 2, 3, 3});
  SegmentIsEqual<std::string>(chunk->GetSegment(2), {"1", "3", "0", "2", "3"});
}

TEST_F(SortOperatorTest, SortBySingleColumnDescending) {
  const auto chunk = GetSortedTable({SortColumnDefinition(2, SortMode::kDescending)})->GetChunk(0);

  SegmentIsEqual<int>(chunk->GetSegment(0), {3, 3, 0, 2, 1});
  SegmentIsEqual<double>(chunk->GetSegment(1), {3, 1, 3, 0, 2});
  SegmentIsEqual<std::string>(chunk->GetSegment(2), {"3", "3", "2", "1", "0"});
}

TEST_F(SortOperatorTest, SortMultiColumn) {
  const auto chunk =
      GetSortedTable({SortColumnDefinition(2, SortMode::kDescending), SortColumnDefinition(1, SortMode::kAscending)})
          ->GetChunk(0);

  SegmentIsEqual<int>(chunk->GetSegment(0), {3, 3, 0, 2, 1});
  SegmentIsEqual<double>(chunk->GetSegment(1), {1, 3, 3, 0, 2});
  SegmentIsEqual<std::string>(chunk->GetSegment(2), {"3", "3", "2", "1", "0"});
}

TEST_F(SortOperatorTest, SortLargeMultiChunkTable) {
  const double scale_factor = 1.5;
  const size_t input_chunk_count = 3;
  const size_t number_of_rows_per_chunk = kChunkDefaultSize * scale_factor;
  const size_t number_of_rows_per_table = number_of_rows_per_chunk * input_chunk_count;
  const size_t expected_number_of_output_chunks =
      number_of_rows_per_table / kChunkDefaultSize + (number_of_rows_per_table % kChunkDefaultSize == 0 ? 0 : 1);

  // (1) Create multiple shuffled chunks.
  std::vector<std::shared_ptr<Chunk>> chunks;
  chunks.reserve(input_chunk_count);

  size_t value = 0;
  for (size_t chunk_id = 0; chunk_id < input_chunk_count; ++chunk_id) {
    std::vector<int> values;
    values.reserve(number_of_rows_per_chunk);
    for (size_t i = 0; i < number_of_rows_per_chunk; ++i) {
      values.push_back(value++);
    }
    std::shuffle(values.begin(), values.end(), std::mt19937(std::random_device()()));

    const auto value_segment_int = std::make_shared<ValueSegment<int>>(std::move(values));
    chunks.push_back(std::make_shared<Chunk>(Segments({value_segment_int})));
  }

  // (2) Create a mock input operator with a data table.
  const TableColumnDefinitions definitions = {TableColumnDefinition("a", DataType::kInt, false)};
  const auto table = std::make_shared<Table>(definitions, std::move(chunks));
  EXPECT_EQ(table->ChunkCount(), input_chunk_count);

  auto mock_input_operator = std::make_shared<TableWrapper>(table);
  mock_input_operator->Execute();

  // (3) Create and run SortOperator.
  const SortColumnDefinition sort_definition(0, SortMode::kAscending);
  const auto sort_operator =
      std::make_shared<SortOperator>(mock_input_operator, std::vector<SortColumnDefinition>{sort_definition});
  sort_operator->Execute(operator_execution_context_);

  // (4) Check output table.
  const auto output_table = sort_operator->GetOutput();
  EXPECT_EQ(output_table->RowCount(), table->RowCount());
  EXPECT_EQ(output_table->ChunkCount(), expected_number_of_output_chunks);

  size_t expected_value = 0;
  for (size_t chunk_id = 0; chunk_id < output_table->ChunkCount(); ++chunk_id) {
    const auto& chunk = output_table->GetChunk(chunk_id);
    if (chunk_id < output_table->ChunkCount() - 1) {
      EXPECT_EQ(chunk->Size(), kChunkDefaultSize);
    }
    const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<int>>(chunk->GetSegment(0));
    for (size_t row_id = 0; row_id < chunk->Size(); ++row_id) {
      EXPECT_EQ(typed_segment->Values()[row_id], expected_value);
      ++expected_value;
    }
  }
}

}  // namespace skyrise
