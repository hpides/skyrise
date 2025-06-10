#include "operator/alias_operator.hpp"

#include <gtest/gtest.h>

#include "operator/table_wrapper.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"

namespace skyrise {

class AliasOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::vector<int> values_int = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<double> values_double = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    std::vector<std::string> values_string = {"0", "1", "2", "3", "4", "5", "6", "7,", "8", "9"};

    const auto value_segment_int = std::make_shared<ValueSegment<int>>(std::move(values_int));
    const auto value_segment_double = std::make_shared<ValueSegment<double>>(std::move(values_double));
    const auto value_segment_string = std::make_shared<ValueSegment<std::string>>(std::move(values_string));
    segments_ = {value_segment_int, value_segment_double, value_segment_string};
    std::vector<std::shared_ptr<Chunk>> chunks = {std::make_shared<Chunk>(segments_),
                                                  std::make_shared<Chunk>(segments_)};

    const TableColumnDefinitions definitions = {TableColumnDefinition("a", DataType::kInt, false),
                                                TableColumnDefinition("b", DataType::kDouble, false),
                                                TableColumnDefinition("c", DataType::kString, true)};
    table_ = std::make_shared<Table>(definitions, std::move(chunks));
    table_wrapper_ = std::make_shared<TableWrapper>(table_);

    // The input operator needs to be executed before the projection operator itself.
    // We do it manually here because the scheduler, which takes care of it normally, is not used here.
    table_wrapper_->Execute();

    column_ids_ = {2, 0, 1};
    aliases_ = {"Column2", "Column0", "Column1"};
    alias_operator_ = std::make_shared<AliasOperator>(table_wrapper_, column_ids_, aliases_);
    alias_operator_->Execute();
  }

  std::shared_ptr<AliasOperator> alias_operator_;
  std::vector<std::string> aliases_;
  std::vector<ColumnId> column_ids_;
  std::shared_ptr<Table> table_;
  std::shared_ptr<TableWrapper> table_wrapper_;
  Segments segments_;
};

TEST_F(AliasOperatorTest, Name) { EXPECT_EQ(alias_operator_->Name(), "Alias"); }

TEST_F(AliasOperatorTest, OrderAndNameColumns) {
  const std::shared_ptr<const Table> table = alias_operator_->GetOutput();
  EXPECT_EQ(table->GetColumnCount(), column_ids_.size());
  EXPECT_EQ(table->ColumnNames(), aliases_);

  const TableColumnDefinitions expected_definitions = {
      TableColumnDefinition("Column2", DataType::kString, true),
      TableColumnDefinition("Column0", DataType::kInt, false),
      TableColumnDefinition("Column1", DataType::kDouble, false),
  };
  EXPECT_EQ(table->ColumnDefinitions(), expected_definitions);

  EXPECT_EQ(table->GetChunk(0)->GetSegment(0), segments_[2]);
  EXPECT_EQ(table->GetChunk(0)->GetSegment(1), segments_[0]);
  EXPECT_EQ(table->GetChunk(0)->GetSegment(2), segments_[1]);
}

}  // namespace skyrise
