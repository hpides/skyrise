/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "operator/filter_operator.hpp"

#include <gtest/gtest.h>

#include "../storage/backend/test_storage.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operator/execution_context.hpp"
#include "operator/table_wrapper.hpp"
#include "scheduler/worker/operator_task.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"
#include "testing/load_table.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

class FilterOperatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table = LoadTable<CsvFormatReader>("csv/input_a.csv", std::make_shared<TestStorage>());
    table_wrapper_ = std::make_shared<TableWrapper>(std::move(table));
    table_wrapper_->Execute(nullptr);

    operator_context_ = std::make_shared<OperatorExecutionContext>(
        nullptr, nullptr, []() { return std::make_shared<FragmentScheduler>(); });
  }

  static void AssertColumnsAreEqual(const std::shared_ptr<const Table>& table, const ColumnId& column_id,
                                    std::vector<AllTypeVariant> expected) {
    for (ChunkId chunk_id = 0; chunk_id < table->ChunkCount(); ++chunk_id) {
      const auto& chunk = table->GetChunk(chunk_id);

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->Size(); ++chunk_offset) {
        const auto& segment = *chunk->GetSegment(column_id);

        const auto& found_value = segment[chunk_offset];
        const auto comparator = [&found_value](const AllTypeVariant& expected_value) {
          // Returns equivalency, not equality to simulate std::multiset.
          return !(found_value < expected_value) && !(expected_value < found_value);
        };

        const auto search_iterator = std::ranges::find_if(expected, comparator);

        ASSERT_TRUE(search_iterator != expected.cend()) << found_value << " not found";
        expected.erase(search_iterator);
      }
    }

    ASSERT_TRUE(expected.empty());
  }

  static std::shared_ptr<AbstractExpression> GetColumnExpression(
      const std::shared_ptr<AbstractOperator>& predecessor_operator, const ColumnId column_id) {
    Assert(predecessor_operator->GetOutput(), "Expected Operator to be executed.");
    const auto& output_table = predecessor_operator->GetOutput();
    const auto& column_definition = output_table->ColumnDefinitions().at(column_id);

    return PqpColumn_(column_id, column_definition.data_type, column_definition.nullable, column_definition.name);
  }

  static std::shared_ptr<FilterOperator> CreateBetweenFilter(
      const std::shared_ptr<AbstractOperator>& predecessor_operator, const ColumnId column_id,
      const AllTypeVariant& lower_bound, const AllTypeVariant& upper_bound,
      const PredicateCondition predicate_condition) {
    const auto column_expression = GetColumnExpression(predecessor_operator, column_id);
    const auto predicate = std::make_shared<BetweenExpression>(predicate_condition, column_expression,
                                                               Value_(lower_bound), Value_(upper_bound));

    return std::make_shared<FilterOperator>(predecessor_operator, predicate);
  }

  static std::shared_ptr<FilterOperator> CreateFilter(
      const std::shared_ptr<AbstractOperator>& predecessor_operator, const ColumnId column_id,
      const PredicateCondition predicate_condition, const AllTypeVariant& left_operand,
      const std::optional<AllTypeVariant>& right_operand = std::nullopt) {
    const auto column_expression = GetColumnExpression(predecessor_operator, column_id);

    if (IsBetweenPredicateCondition(predicate_condition)) {
      Assert(right_operand.has_value(), "Upper bound not set for between predicate condition.");
      return CreateBetweenFilter(predecessor_operator, column_id, left_operand, *right_operand, predicate_condition);
    }

    std::shared_ptr<AbstractExpression> predicate;
    if (predicate_condition == PredicateCondition::kIsNull || predicate_condition == PredicateCondition::kIsNotNull) {
      predicate = std::make_shared<IsNullExpression>(predicate_condition, column_expression);
    } else {
      predicate =
          std::make_shared<BinaryPredicateExpression>(predicate_condition, column_expression, Value_(left_operand));
    }

    return std::make_shared<FilterOperator>(predecessor_operator, predicate);
  }

  std::shared_ptr<OperatorExecutionContext> operator_context_;
  std::shared_ptr<TableWrapper> table_wrapper_;
};

TEST_F(FilterOperatorTest, IntScanGreaterThanEquals) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{0}, PredicateCondition::kGreaterThanEquals, 2);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{0}, {2, 3, 4});
}

TEST_F(FilterOperatorTest, IntScanLessThan) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{0}, PredicateCondition::kLessThan, 2);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{0}, {1});
}

TEST_F(FilterOperatorTest, IntScanAllMatch) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{0}, PredicateCondition::kGreaterThanEquals, 0);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{0}, {1, 2, 3, 4});
}

TEST_F(FilterOperatorTest, IntScanNoMatch) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{0}, PredicateCondition::kGreaterThanEquals, 15);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{0}, {});
}

TEST_F(FilterOperatorTest, IntScanBetweenInclusive) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{0}, PredicateCondition::kBetweenInclusive, 1, 3);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{0}, {1, 2, 3});
}

TEST_F(FilterOperatorTest, IntScanBetweenExclusive) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{0}, PredicateCondition::kBetweenExclusive, 1, 3);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{0}, {2});
}

TEST_F(FilterOperatorTest, IntScanNotEquals) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{0}, PredicateCondition::kNotEquals, 3);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{0}, {1, 2, 4});
}

TEST_F(FilterOperatorTest, MultiChunkParallelScan) {
  const int64_t row_count = kChunkDefaultSize;
  const int64_t less_than_equals_value = row_count / 2;

  std::vector<int64_t> values;
  values.reserve(row_count);

  for (int64_t i = 0; i < row_count; ++i) {
    values.push_back(i);
  }

  // Create table with generated values.
  const auto value_segment = std::make_shared<ValueSegment<int64_t>>(std::move(values));
  const auto chunk = std::make_shared<Chunk>(Segments({value_segment}));
  std::vector<std::shared_ptr<Chunk>> chunks = {chunk, chunk, chunk};
  const TableColumnDefinitions definitions = {TableColumnDefinition("a", DataType::kLong, false)};
  const auto table = std::make_shared<Table>(definitions, std::move(chunks));

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->Execute(nullptr);

  // Create filter operator.
  const auto filter_operator =
      CreateFilter(table_wrapper, ColumnId{0}, PredicateCondition::kLessThanEquals, less_than_equals_value);

  const std::vector<std::shared_ptr<skyrise::AbstractTask>> tasks =
      OperatorTask::GenerateTasksFromOperator(filter_operator, operator_context_).first;

  operator_context_->GetScheduler()->ScheduleAndWaitForTasks(tasks);

  EXPECT_EQ(filter_operator->GetOutput()->RowCount(), table->ChunkCount() * (less_than_equals_value + 1));
  EXPECT_EQ(filter_operator->GetOutput()->ChunkCount(), table->ChunkCount());
}

TEST_F(FilterOperatorTest, StringEquals) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{3}, PredicateCondition::kEquals, "Hello");
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{3}, {"Hello"});
}

TEST_F(FilterOperatorTest, StringNotEquals) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{3}, PredicateCondition::kNotEquals, "Hello");
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{3}, {"a", "what", "Same"});
}

TEST_F(FilterOperatorTest, StringLessThan) {
  // Note: Lowercase "a" > "H" and "S" but < "w".
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{3}, PredicateCondition::kLessThan, "a");
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{3}, {"Hello", "Same"});
}

TEST_F(FilterOperatorTest, DatesLessThan) {
  const auto filter_operator = CreateFilter(table_wrapper_, ColumnId{5}, PredicateCondition::kLessThan, "2012-01-01");
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{5}, {"2011-09-03", "2010-01-02"});
}

TEST_F(FilterOperatorTest, FloatScanBetweenInclusive) {
  // Note: If the columns data type is float, it is important to explicitly pass float values as constraints.
  const auto filter_operator =
      CreateFilter(table_wrapper_, ColumnId{6}, PredicateCondition::kBetweenInclusive, 20.5f, 50.0f);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{6}, {20.5f, 42.8f});
}

TEST_F(FilterOperatorTest, FloatScanBetweenExclusive) {
  // Note: If the columns data type is float, it is important to explicitly pass float values as constraints.
  const auto filter_operator =
      CreateFilter(table_wrapper_, ColumnId{6}, PredicateCondition::kBetweenExclusive, 20.5f, 50.0f);
  filter_operator->Execute(operator_context_);

  AssertColumnsAreEqual(filter_operator->GetOutput(), ColumnId{6}, {42.8f});
}

}  // namespace skyrise
