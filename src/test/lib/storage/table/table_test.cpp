/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "storage/table/table.hpp"

#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "all_type_variant.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class TableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    column_definitions_.emplace_back("column_1", DataType::kInt, false);
    column_definitions_.emplace_back("column_2", DataType::kString, true);
    table_ = std::make_shared<Table>(column_definitions_);
  }

  std::shared_ptr<Table> CreateTableWithNChunks(size_t n_chunks) {
    Segments segments;
    segments.push_back(std::make_shared<ValueSegment<int>>(1));
    segments.push_back(std::make_shared<ValueSegment<std::string>>("Hello World!"));

    std::vector<std::shared_ptr<Chunk>> chunks_vector;
    chunks_vector.reserve(n_chunks);
    for (size_t i = 0; i < n_chunks; ++i) {
      chunks_vector.push_back(std::make_shared<Chunk>(segments));
    }
    return std::make_shared<Table>(column_definitions_, std::move(chunks_vector));
  }

  std::shared_ptr<Table> table_;
  TableColumnDefinitions column_definitions_;
};

TEST_F(TableTest, GetColumnName) {
  EXPECT_EQ(table_->ColumnName(ColumnId{0}), "column_1");
  EXPECT_EQ(table_->ColumnName(ColumnId{1}), "column_2");
}

TEST_F(TableTest, GetColumnDataType) {
  EXPECT_EQ(table_->ColumnDataType(ColumnId{0}), DataType::kInt);
  EXPECT_EQ(table_->ColumnDataType(ColumnId{1}), DataType::kString);
}

TEST_F(TableTest, GetColumnIdByName) {
  EXPECT_EQ(table_->ColumnIdByName("column_2"), 1);
  EXPECT_THROW(table_->ColumnIdByName("no_column_name"), std::exception);
}

TEST_F(TableTest, EmplaceChunk) {
  EXPECT_EQ(table_->ChunkCount(), 0);

  auto value_segment_integer = std::make_shared<ValueSegment<int>>();
  auto value_segment_string = std::make_shared<ValueSegment<std::string>>();

  value_segment_integer->Append(5);
  value_segment_string->Append("five");

  table_->AppendChunk({value_segment_integer, value_segment_string});
  EXPECT_EQ(table_->ChunkCount(), 1);
  EXPECT_EQ(table_->GetChunk(0)->GetColumnCount(), 2);
  EXPECT_EQ(table_->GetChunk(0)->GetSegment(ColumnId{0})->Size(), 1);
}

TEST_F(TableTest, EmplaceEmptyChunk) {
  EXPECT_EQ(table_->ChunkCount(), 0);

  auto value_segment_integer = std::make_shared<ValueSegment<int>>();
  auto value_segment_string = std::make_shared<ValueSegment<std::string>>();

  table_->AppendChunk({value_segment_integer, value_segment_string});
  EXPECT_EQ(table_->ChunkCount(), 1);
}

TEST_F(TableTest, RowCount) {
  EXPECT_EQ(table_->ChunkCount(), 0);

  auto value_segment_integer = std::make_shared<ValueSegment<int>>();
  auto value_segment_string = std::make_shared<ValueSegment<std::string>>();

  const size_t expected_row_count = 10;
  for (size_t i = 0; i < expected_row_count; ++i) {
    value_segment_integer->Append(1);
    value_segment_string->Append("Hello World");
  }

  table_->AppendChunk({value_segment_integer, value_segment_string});
  EXPECT_EQ(table_->RowCount(), expected_row_count);
}

TEST_F(TableTest, GetColumnCount) { EXPECT_EQ(table_->GetColumnCount(), 2); }

TEST_F(TableTest, ChunkCount) {
  const size_t number_chunks = 5;
  const std::shared_ptr<Table> table = CreateTableWithNChunks(number_chunks);

  EXPECT_EQ(table->ChunkCount(), 5);
}

TEST_F(TableTest, GetChunk) {
  const size_t number_chunks = 2;
  const std::shared_ptr<Table> table = CreateTableWithNChunks(number_chunks);

  ASSERT_EQ(table->ChunkCount(), 2);
  EXPECT_NE(table->GetChunk(ChunkId{0}), nullptr);
  EXPECT_NE(table->GetChunk(ChunkId{1}), nullptr);
}

TEST_F(TableTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations.
   */

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column_1", DataType::kString, true);
  column_definitions.emplace_back("column_2", DataType::kString, true);

  auto table = std::make_shared<Table>(column_definitions);

  const auto empty_memory_usage = table->MemoryUsageBytes();

  auto vs_str_one = std::make_shared<ValueSegment<std::string>>();
  auto vs_str_two = std::make_shared<ValueSegment<std::string>>();

  vs_str_one->Append("Hello");
  vs_str_two->Append("Hello");

  table->AppendChunk({vs_str_one, vs_str_two});

  EXPECT_GT(table->MemoryUsageBytes(), empty_memory_usage + (2 * (sizeof(std::string))));
}

TEST_F(TableTest, AppendToChunksVectorConncurrently) {
  EXPECT_EQ(table_->ChunkCount(), 0);

  auto value_segment_integer = std::make_shared<ValueSegment<int>>();
  auto value_segment_string = std::make_shared<ValueSegment<std::string>>();

  value_segment_integer->Append(5);
  value_segment_string->Append("five");

  std::vector<std::thread> threads(10);
  for (auto& thread : threads) {
    thread = std::thread([&, this]() { table_->AppendChunk({value_segment_integer, value_segment_string}); });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(table_->ChunkCount(), 10);
}

}  // namespace skyrise
