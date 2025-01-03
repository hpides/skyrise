#include "operator/partition_operator.hpp"

#include <gtest/gtest.h>

#include "operator/table_wrapper.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class PartitionOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::vector<int> values_int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<double> values_double{10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    std::vector<std::string> values_string{"0", "1", "2", "3", "4", "5", "6", "7,", "8", "9"};

    const auto value_segment_int = std::make_shared<ValueSegment<int>>(std::move(values_int));
    const auto value_segment_double = std::make_shared<ValueSegment<double>>(std::move(values_double));
    const auto value_segment_string = std::make_shared<ValueSegment<std::string>>(std::move(values_string));

    std::vector<std::shared_ptr<Chunk>> chunks{
        std::make_shared<Chunk>(Segments({value_segment_int, value_segment_double, value_segment_string}))};

    const TableColumnDefinitions definitions = {TableColumnDefinition("a", DataType::kInt, false),
                                                TableColumnDefinition("b", DataType::kDouble, false),
                                                TableColumnDefinition("c", DataType::kString, false)};
    table_ = std::make_shared<Table>(definitions, std::move(chunks));
  }

  std::shared_ptr<Table> table_;
};

TEST_F(PartitionOperatorTest, HashPartitioning) {
  const auto table_wrapper = std::make_shared<TableWrapper>(table_);

  const auto partitioning_function = std::make_shared<HashPartitioningFunction>(std::set<ColumnId>{0, 2}, 4);
  const auto partition = std::make_shared<PartitionOperator>(table_wrapper, partitioning_function);

  EXPECT_EQ(partition->Name(), "Partition");

  table_wrapper->Execute();
  partition->Execute();

  const auto result = partition->GetOutput();

  EXPECT_EQ(result->ChunkCount(), 4);
  EXPECT_EQ(result->GetColumnCount(), 3);
  EXPECT_EQ(result->RowCount(), 10);
}

}  // namespace skyrise
