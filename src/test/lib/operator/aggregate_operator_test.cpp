#include <memory>
#include <optional>
#include <regex>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "expression/aggregate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/string_agg_aggregate_expression.hpp"
#include "operator/abstract_operator.hpp"
#include "operator/aggregate_hash_operator.hpp"
#include "operator/table_wrapper.hpp"
#include "scheduler/worker/operator_task.hpp"
#include "storage/backend/testdata_storage.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/table/table.hpp"
#include "testing/load_table.hpp"
#include "testing/testing_assert.hpp"
#include "types.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

class AggregateOperatorTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    auto storage = std::make_shared<TestdataStorage>();

    table_configuration_.delimiter = '|';
    table_configuration_.has_types = true;
    table_configuration_.has_header = true;
    table_configuration_.guess_delimiter = false;
    table_configuration_.guess_has_header = false;
    table_configuration_.guess_has_types = false;

    table_1_1_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_int_1gb_1agg/input.tbl", storage, table_configuration_));
    table_1_1_->Execute(nullptr);

    table_2_1_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_int_2gb_1agg/input.tbl", storage, table_configuration_));
    table_2_1_->Execute(nullptr);

    table_1_2_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_int_1gb_2agg/input.tbl", storage, table_configuration_));
    table_1_2_->Execute(nullptr);

    table_1_0_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_int_1gb_0agg/input.tbl", storage, table_configuration_));
    table_1_0_->Execute(nullptr);

    table_1_1_large_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_int_1gb_1agg/input_large.tbl", storage, table_configuration_));
    table_1_1_large_->Execute(nullptr);

    table_2_2_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_int_2gb_2agg/input.tbl", storage, table_configuration_));
    table_2_2_->Execute(nullptr);

    table_2_2_filtered_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_int_2gb_2agg/input_filtered.tbl", storage, table_configuration_));
    table_2_2_filtered_->Execute(nullptr);

    table_3_1_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_int_3gb_1agg/input.tbl", storage, table_configuration_));
    table_3_1_->Execute(nullptr);

    table_1_1_string_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_string_1gb_1agg/input.tbl", storage, table_configuration_));
    table_1_1_string_->Execute(nullptr);

    table_string_agg_ = std::make_shared<TableWrapper>(LoadTable<CsvFormatReader>(
        "tbl/aggregate_operator/groupby_string_agg/input.tbl", storage, table_configuration_));
    table_string_agg_->Execute(nullptr);
  }

  template <typename T>
  void TestOutput(const std::shared_ptr<AbstractOperator>& input_operator,
                  const std::vector<std::pair<ColumnId, AggregateFunction>>& aggregate_definitions,
                  const std::vector<ColumnId>& groupby_column_ids, const std::string& path,
                  const std::optional<std::vector<SortColumnDefinition>>& sort_column_definitions = std::nullopt) {
    std::shared_ptr<Table> expected_result = LoadTable<CsvFormatReader>(path, storage_, table_configuration_);
    // TODO(tobodner): Load Tables with nullable columns
    TableColumnDefinitions expected_result_schema;
    expected_result_schema.reserve(expected_result->ColumnDefinitions().size());
    for (const auto& table_definition : expected_result->ColumnDefinitions()) {
      if (std::regex_match(table_definition.name, std::regex("(SUM|MAX|MIN|AVG|STDDEV_SAMP|STRING_AGG)\\(.*\\)"))) {
        expected_result_schema.emplace_back(table_definition.name, table_definition.data_type, true);
      } else {
        expected_result_schema.push_back(table_definition);
      }
    }

    std::vector<std::shared_ptr<Chunk>> chunks;
    chunks.reserve(expected_result->ChunkCount());
    for (size_t i = 0; i < expected_result->ChunkCount(); ++i) {
      chunks.push_back(expected_result->GetChunk(i));
    }
    expected_result = std::make_shared<Table>(expected_result_schema, std::move(chunks));

    std::vector<std::shared_ptr<AggregateExpression>> aggregates;
    const auto& table = input_operator->GetOutput();
    for (const auto& [column_id, aggregate_function] : aggregate_definitions) {
      if (aggregate_function == AggregateFunction::kStringAgg) {
        aggregates.push_back(std::make_shared<StringAggAggregateExpression>(
            PqpColumn_(column_id, table->ColumnDataType(column_id), table->ColumnIsNullable(column_id),
                       table->ColumnName(column_id)),
            sort_column_definitions));
      } else {
        if (column_id != kInvalidColumnId) {
          aggregates.push_back(std::make_shared<AggregateExpression>(
              aggregate_function, PqpColumn_(column_id, table->ColumnDataType(column_id),
                                             table->ColumnIsNullable(column_id), table->ColumnName(column_id))));
        } else {
          aggregates.push_back(std::make_shared<AggregateExpression>(
              aggregate_function, PqpColumn_(column_id, DataType::kLong, false, "*")));
        }
      }
    }

    {
      // Test the Aggregate on stored table data.
      const auto aggregate = std::make_shared<AggregateHashOperator>(input_operator, aggregates, groupby_column_ids);
      aggregate->Execute(operator_execution_context_);

      EXPECT_TABLE_EQ_UNORDERED(aggregate->GetOutput(), expected_result);
    }
  }

 protected:
  void SetUp() override {
    storage_ = std::make_shared<TestdataStorage>();
    scheduler_ = std::make_shared<FragmentScheduler>();
    operator_execution_context_ =
        std::make_shared<OperatorExecutionContext>(nullptr, nullptr, [this]() { return scheduler_; });
  }

  inline static CsvFormatReaderOptions table_configuration_;

  std::shared_ptr<TestdataStorage> storage_;

  inline static std::shared_ptr<TableWrapper> table_1_0_, table_1_1_, table_1_1_large_, table_1_2_, table_2_1_,
      table_2_2_, table_2_2_filtered_, table_3_1_, table_1_1_string_, table_string_agg_;

  std::shared_ptr<FragmentScheduler> scheduler_;
  std::shared_ptr<OperatorExecutionContext> operator_execution_context_;
};

TEST_F(AggregateOperatorTest, OperatorName) {
  const auto table = table_1_1_->GetOutput();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      Max_(PqpColumn_(ColumnId{1}, table->ColumnDataType(ColumnId{1}), table->ColumnIsNullable(ColumnId{1}),
                      table->ColumnName(ColumnId{1})))};
  const auto aggregate =
      std::make_shared<AggregateHashOperator>(table_1_1_, aggregate_expressions, std::vector<ColumnId>{ColumnId{0}});

  EXPECT_EQ(aggregate->Name(), "AggregateHash");
}

TEST_F(AggregateOperatorTest, CannotSumStringColumns) {
  const auto table = table_1_1_string_->GetOutput();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      Sum_(PqpColumn_(ColumnId{0}, table->ColumnDataType(ColumnId{0}), table->ColumnIsNullable(ColumnId{0}),
                      table->ColumnName(ColumnId{0})))};
  const auto aggregate = std::make_shared<AggregateHashOperator>(table_1_1_string_, aggregate_expressions,
                                                                 std::vector<ColumnId>{ColumnId{0}});

  EXPECT_THROW(aggregate->Execute(operator_execution_context_), std::logic_error);
}

TEST_F(AggregateOperatorTest, CannotAvgStringColumns) {
  const auto table = table_1_1_string_->GetOutput();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      Avg_(PqpColumn_(ColumnId{0}, table->ColumnDataType(ColumnId{0}), table->ColumnIsNullable(ColumnId{0}),
                      table->ColumnName(ColumnId{0})))};
  const auto aggregate = std::make_shared<AggregateHashOperator>(table_1_1_string_, aggregate_expressions,
                                                                 std::vector<ColumnId>{ColumnId{0}});

  EXPECT_THROW(aggregate->Execute(operator_execution_context_), std::logic_error);
}

TEST_F(AggregateOperatorTest, CannotStandardDeviationSampleStringColumns) {
  const auto table = table_1_1_string_->GetOutput();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      StandardDeviationSample_(PqpColumn_(ColumnId{0}, table->ColumnDataType(ColumnId{0}),
                                          table->ColumnIsNullable(ColumnId{0}), table->ColumnName(ColumnId{0})))};
  const auto aggregate = std::make_shared<AggregateHashOperator>(table_1_1_string_, aggregate_expressions,
                                                                 std::vector<ColumnId>{ColumnId{0}});

  EXPECT_THROW(aggregate->Execute(operator_execution_context_), std::logic_error);
}

/*
 * The ANY aggregation is a special case which is used to obtain "any value" of a group of which we know that each
 * value in this group is the same (for most cases, the group will have a size of one). This can be the case, when
 * the aggregated column is functionally dependent on the group-by columns.
 */
TEST_F(AggregateOperatorTest, AnyOnGroupWithMultipleEntries) {
  const auto table = table_2_2_filtered_->GetOutput();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      Any_(PqpColumn_(ColumnId{2}, table->ColumnDataType(ColumnId{2}), table->ColumnIsNullable(ColumnId{2}),
                      table->ColumnName(ColumnId{2})))};

  const auto aggregate = std::make_shared<AggregateHashOperator>(table_2_2_filtered_, aggregate_expressions,
                                                                 std::vector<ColumnId>{ColumnId{0}, ColumnId{1}});

  const std::vector<std::shared_ptr<AbstractTask>> tasks =
      OperatorTask::GenerateTasksFromOperator(aggregate, operator_execution_context_).first;

  scheduler_->ScheduleAndWaitForTasks(tasks);

  const auto segment = aggregate->GetOutput()->GetChunk(0)->GetSegment(2);

  EXPECT_EQ(std::get<int32_t>((*segment)[0]), 20);
}

TEST_F(AggregateOperatorTest, CanCountStringColumns) {
  TestOutput<AggregateHashOperator>(table_1_1_string_, {{ColumnId{0}, AggregateFunction::kCount}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_string_1gb_1agg/count_str.tbl");
}

TEST_F(AggregateOperatorTest, SingleAggregateMax) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kMax}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/max.tbl");
}

TEST_F(AggregateOperatorTest, SingleAggregateMin) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kMin}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/min.tbl");
}

TEST_F(AggregateOperatorTest, SingleAggregateSum) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kSum}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/sum.tbl");
}

TEST_F(AggregateOperatorTest, SingleAggregateAvg) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kAvg}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/avg.tbl");
}

TEST_F(AggregateOperatorTest, SingleAggregateStandardDeviationSampleLarge) {
  TestOutput<AggregateHashOperator>(table_1_1_large_, {{ColumnId{1}, AggregateFunction::kStandardDeviationSample}},
                                    {ColumnId{0}}, "tbl/aggregate_operator/groupby_int_1gb_1agg/stddev_samp_large.tbl");
}

TEST_F(AggregateOperatorTest, SingleAggregateCount) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kCount}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/count.tbl");
}

TEST_F(AggregateOperatorTest, SingleAggregateCountDistinct) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kCountDistinct}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/count_distinct.tbl");
}

TEST_F(AggregateOperatorTest, StringSingleAggregateMax) {
  TestOutput<AggregateHashOperator>(table_1_1_string_, {{ColumnId{1}, AggregateFunction::kMax}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_string_1gb_1agg/max.tbl");
}

TEST_F(AggregateOperatorTest, StringSingleAggregateMin) {
  TestOutput<AggregateHashOperator>(table_1_1_string_, {{ColumnId{1}, AggregateFunction::kMin}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_string_1gb_1agg/min.tbl");
}

TEST_F(AggregateOperatorTest, StringSingleAggregateStringMax) {
  TestOutput<AggregateHashOperator>(table_1_1_string_, {{ColumnId{0}, AggregateFunction::kMax}}, {},
                                    "tbl/aggregate_operator/groupby_string_1gb_1agg/max_str.tbl");
}

TEST_F(AggregateOperatorTest, StringSingleAggregateStringMin) {
  TestOutput<AggregateHashOperator>(table_1_1_string_, {{ColumnId{0}, AggregateFunction::kMin}}, {},
                                    "tbl/aggregate_operator/groupby_string_1gb_1agg/min_str.tbl");
}

TEST_F(AggregateOperatorTest, StringSingleAggregateSum) {
  TestOutput<AggregateHashOperator>(table_1_1_string_, {{ColumnId{1}, AggregateFunction::kSum}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_string_1gb_1agg/sum.tbl");
}

TEST_F(AggregateOperatorTest, StringSingleAggregateAvg) {
  TestOutput<AggregateHashOperator>(table_1_1_string_, {{ColumnId{1}, AggregateFunction::kAvg}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_string_1gb_1agg/avg.tbl");
}

TEST_F(AggregateOperatorTest, StringSingleAggregateCount) {
  TestOutput<AggregateHashOperator>(table_1_1_string_, {{ColumnId{1}, AggregateFunction::kCount}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_string_1gb_1agg/count.tbl");
}

TEST_F(AggregateOperatorTest, DictionarySingleAggregateMax) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kMax}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/max.tbl");
}

TEST_F(AggregateOperatorTest, DictionarySingleAggregateMin) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kMin}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/min.tbl");
}

TEST_F(AggregateOperatorTest, DictionarySingleAggregateSum) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kSum}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/sum.tbl");
}

TEST_F(AggregateOperatorTest, DictionarySingleAggregateAvg) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kAvg}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/avg.tbl");
}

TEST_F(AggregateOperatorTest, DictionarySingleAggregateCount) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kCount}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/count.tbl");
}

TEST_F(AggregateOperatorTest, TwoAggregateAvgMax) {
  TestOutput<AggregateHashOperator>(table_1_2_,
                                    {{ColumnId{1}, AggregateFunction::kMax}, {ColumnId{2}, AggregateFunction::kAvg}},
                                    {ColumnId{0}}, "tbl/aggregate_operator/groupby_int_1gb_2agg/max_avg.tbl");
}

TEST_F(AggregateOperatorTest, TwoAggregateMinAvg) {
  TestOutput<AggregateHashOperator>(table_1_2_,
                                    {{ColumnId{1}, AggregateFunction::kMin}, {ColumnId{2}, AggregateFunction::kAvg}},
                                    {ColumnId{0}}, "tbl/aggregate_operator/groupby_int_1gb_2agg/min_avg.tbl");
}

TEST_F(AggregateOperatorTest, TwoAggregateMinMax) {
  TestOutput<AggregateHashOperator>(table_1_2_,
                                    {{ColumnId{1}, AggregateFunction::kMin}, {ColumnId{2}, AggregateFunction::kMax}},
                                    {ColumnId{0}}, "tbl/aggregate_operator/groupby_int_1gb_2agg/min_max.tbl");
}

TEST_F(AggregateOperatorTest, TwoAggregateAvgAvg) {
  TestOutput<AggregateHashOperator>(table_1_2_,
                                    {{ColumnId{1}, AggregateFunction::kAvg}, {ColumnId{2}, AggregateFunction::kAvg}},
                                    {ColumnId{0}}, "tbl/aggregate_operator/groupby_int_1gb_2agg/avg_avg.tbl");
}

TEST_F(AggregateOperatorTest, TwoAggregateSumAvg) {
  TestOutput<AggregateHashOperator>(table_1_2_,
                                    {{ColumnId{1}, AggregateFunction::kSum}, {ColumnId{2}, AggregateFunction::kAvg}},
                                    {ColumnId{0}}, "tbl/aggregate_operator/groupby_int_1gb_2agg/sum_avg.tbl");
}

TEST_F(AggregateOperatorTest, TwoAggregateSumSum) {
  TestOutput<AggregateHashOperator>(table_1_2_,
                                    {{ColumnId{1}, AggregateFunction::kSum}, {ColumnId{2}, AggregateFunction::kSum}},
                                    {ColumnId{0}}, "tbl/aggregate_operator/groupby_int_1gb_2agg/sum_sum.tbl");
}

TEST_F(AggregateOperatorTest, TwoAggregateSumCount) {
  TestOutput<AggregateHashOperator>(table_1_2_,
                                    {{ColumnId{1}, AggregateFunction::kSum}, {ColumnId{2}, AggregateFunction::kCount}},
                                    {ColumnId{0}}, "tbl/aggregate_operator/groupby_int_1gb_2agg/sum_count.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyMax) {
  TestOutput<AggregateHashOperator>(table_2_1_, {{ColumnId{2}, AggregateFunction::kMax}}, {ColumnId{0}, ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_int_2gb_1agg/max.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyMin) {
  TestOutput<AggregateHashOperator>(table_2_1_, {{ColumnId{2}, AggregateFunction::kMin}}, {ColumnId{0}, ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_int_2gb_1agg/min.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbySum) {
  TestOutput<AggregateHashOperator>(table_2_1_, {{ColumnId{2}, AggregateFunction::kSum}}, {ColumnId{0}, ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_int_2gb_1agg/sum.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyAvg) {
  TestOutput<AggregateHashOperator>(table_2_1_, {{ColumnId{2}, AggregateFunction::kAvg}}, {ColumnId{0}, ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_int_2gb_1agg/avg.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyCount) {
  TestOutput<AggregateHashOperator>(table_2_1_, {{ColumnId{2}, AggregateFunction::kCount}}, {ColumnId{0}, ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_int_2gb_1agg/count.tbl");
}

TEST_F(AggregateOperatorTest, ThreeGroupbyMax) {
  TestOutput<AggregateHashOperator>(table_3_1_, {{ColumnId{2}, AggregateFunction::kMax}},
                                    {ColumnId{0}, ColumnId{1}, ColumnId{3}},
                                    "tbl/aggregate_operator/groupby_int_3gb_1agg/max.tbl");
}

TEST_F(AggregateOperatorTest, ThreeGroupbyMin) {
  TestOutput<AggregateHashOperator>(table_3_1_, {{ColumnId{2}, AggregateFunction::kMin}},
                                    {ColumnId{0}, ColumnId{1}, ColumnId{3}},
                                    "tbl/aggregate_operator/groupby_int_3gb_1agg/min.tbl");
}

TEST_F(AggregateOperatorTest, ThreeGroupbySum) {
  TestOutput<AggregateHashOperator>(table_3_1_, {{ColumnId{2}, AggregateFunction::kSum}},
                                    {ColumnId{0}, ColumnId{1}, ColumnId{3}},
                                    "tbl/aggregate_operator/groupby_int_3gb_1agg/sum.tbl");
}

TEST_F(AggregateOperatorTest, ThreeGroupbyAvg) {
  TestOutput<AggregateHashOperator>(table_3_1_, {{ColumnId{2}, AggregateFunction::kAvg}},
                                    {ColumnId{0}, ColumnId{1}, ColumnId{3}},
                                    "tbl/aggregate_operator/groupby_int_3gb_1agg/avg.tbl");
}

TEST_F(AggregateOperatorTest, ThreeGroupbyCount) {
  TestOutput<AggregateHashOperator>(table_3_1_, {{ColumnId{2}, AggregateFunction::kCount}},
                                    {ColumnId{0}, ColumnId{1}, ColumnId{3}},
                                    "tbl/aggregate_operator/groupby_int_3gb_1agg/count.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyAndTwoAggregateMaxAvg) {
  TestOutput<AggregateHashOperator>(
      table_2_2_, {{ColumnId{2}, AggregateFunction::kMax}, {ColumnId{3}, AggregateFunction::kAvg}},
      {ColumnId{0}, ColumnId{1}}, "tbl/aggregate_operator/groupby_int_2gb_2agg/max_avg.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyAndTwoAggregateMinAvg) {
  TestOutput<AggregateHashOperator>(
      table_2_2_, {{ColumnId{2}, AggregateFunction::kMin}, {ColumnId{3}, AggregateFunction::kAvg}},
      {ColumnId{0}, ColumnId{1}}, "tbl/aggregate_operator/groupby_int_2gb_2agg/min_avg.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyAndTwoAggregateMinMax) {
  TestOutput<AggregateHashOperator>(
      table_2_2_, {{ColumnId{2}, AggregateFunction::kMin}, {ColumnId{3}, AggregateFunction::kMax}},
      {ColumnId{0}, ColumnId{1}}, "tbl/aggregate_operator/groupby_int_2gb_2agg/min_max.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyAndTwoAggregateSumAvg) {
  TestOutput<AggregateHashOperator>(
      table_2_2_, {{ColumnId{2}, AggregateFunction::kSum}, {ColumnId{3}, AggregateFunction::kAvg}},
      {ColumnId{0}, ColumnId{1}}, "tbl/aggregate_operator/groupby_int_2gb_2agg/sum_avg.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyAndTwoAggregateSumSum) {
  TestOutput<AggregateHashOperator>(
      table_2_2_, {{ColumnId{2}, AggregateFunction::kSum}, {ColumnId{3}, AggregateFunction::kSum}},
      {ColumnId{0}, ColumnId{1}}, "tbl/aggregate_operator/groupby_int_2gb_2agg/sum_sum.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyAndTwoAggregateSumCount) {
  TestOutput<AggregateHashOperator>(
      table_2_2_, {{ColumnId{2}, AggregateFunction::kSum}, {ColumnId{3}, AggregateFunction::kCount}},
      {ColumnId{0}, ColumnId{1}}, "tbl/aggregate_operator/groupby_int_2gb_2agg/sum_count.tbl");
}

TEST_F(AggregateOperatorTest, NoGroupbySingleAggregateMax) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kMax}}, {},
                                    "tbl/aggregate_operator/0gb_1agg/max.tbl");
}

TEST_F(AggregateOperatorTest, NoGroupbySingleAggregateMin) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kMin}}, {},
                                    "tbl/aggregate_operator/0gb_1agg/min.tbl");
}

TEST_F(AggregateOperatorTest, NoGroupbySingleAggregateSum) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kSum}}, {},
                                    "tbl/aggregate_operator/0gb_1agg/sum.tbl");
}

TEST_F(AggregateOperatorTest, NoGroupbySingleAggregateAvg) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kAvg}}, {},
                                    "tbl/aggregate_operator/0gb_1agg/avg.tbl");
}

TEST_F(AggregateOperatorTest, NoGroupbySingleAggregateStandardDeviationSample) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kStandardDeviationSample}}, {},
                                    "tbl/aggregate_operator/0gb_1agg/stddev_samp.tbl");
}

TEST_F(AggregateOperatorTest, NoGroupbySingleAggregateCount) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{ColumnId{1}, AggregateFunction::kCount}}, {},
                                    "tbl/aggregate_operator/0gb_1agg/count.tbl");
}

TEST_F(AggregateOperatorTest, OneGroupbyAndNoAggregate) {
  TestOutput<AggregateHashOperator>(table_1_0_, {}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_0agg/result.tbl");
}

TEST_F(AggregateOperatorTest, TwoGroupbyAndNoAggregate) {
  TestOutput<AggregateHashOperator>(table_1_1_, {}, {ColumnId{0}, ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_int_2gb_0agg/result.tbl");
}

TEST_F(AggregateOperatorTest, NoGroupbyAndNoAggregate) {
  EXPECT_THROW(std::make_shared<AggregateHashOperator>(
                   table_1_1_string_, std::vector<std::shared_ptr<AggregateExpression>>{}, std::vector<ColumnId>{}),
               std::logic_error);
}

TEST_F(AggregateOperatorTest, OneGroupbyCountStar) {
  TestOutput<AggregateHashOperator>(table_1_1_, {{kInvalidColumnId, AggregateFunction::kCount}}, {ColumnId{0}},
                                    "tbl/aggregate_operator/groupby_int_1gb_1agg/count_star.tbl");
}

TEST_F(AggregateOperatorTest, TestStringAggWithoutSorting) {
  /**
   * This test aggregates the strings of column 0 grouped by column 1. Strings are not sorted prior aggregation.
   */
  TestOutput<AggregateHashOperator>(table_string_agg_, {{ColumnId{0}, AggregateFunction::kStringAgg}}, {ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_string_agg/result.tbl");
}

TEST_F(AggregateOperatorTest, TestStringAggSortAscending) {
  /**
   * This test aggregates the strings of column 0 grouped by column 1 and sorted in ascending order by column 2.
   */
  std::vector<SortColumnDefinition> sort_column_definitions({SortColumnDefinition(ColumnId(2), SortMode::kAscending)});
  TestOutput<AggregateHashOperator>(table_string_agg_, {{ColumnId{0}, AggregateFunction::kStringAgg}}, {ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_string_agg/result_sorted_ascending.tbl",
                                    sort_column_definitions);
}

TEST_F(AggregateOperatorTest, TestStringAggSortDescending) {
  /**
   * This test aggregates the strings of column 0 grouped by column 1 and sorted in descending order by column 2.
   */
  std::vector<SortColumnDefinition> sort_column_definitions({SortColumnDefinition(ColumnId(2), SortMode::kDescending)});
  TestOutput<AggregateHashOperator>(table_string_agg_, {{ColumnId{0}, AggregateFunction::kStringAgg}}, {ColumnId{1}},
                                    "tbl/aggregate_operator/groupby_string_agg/result_sorted_descending.tbl",
                                    sort_column_definitions);
}

TEST_F(AggregateOperatorTest, TestStringAggSortAscendingMultiple) {
  /**
   * This test aggregates the strings of column 0 grouped by column 1 and sorted in ascending order by columns 2 and 0.
   */
  std::vector<SortColumnDefinition> sort_column_definitions({SortColumnDefinition(ColumnId(2), SortMode::kAscending),
                                                             SortColumnDefinition(ColumnId(0), SortMode::kAscending)});
  TestOutput<AggregateHashOperator>(
      table_string_agg_, {{ColumnId{0}, AggregateFunction::kStringAgg}}, {ColumnId{1}},
      "tbl/aggregate_operator/groupby_string_agg/result_sorted_ascending_multi_column.tbl", sort_column_definitions);
}

TEST_F(AggregateOperatorTest, TestStringAggWithFloatColumn) {
  const auto table = table_string_agg_->GetOutput();
  const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{
      String_Agg_(PqpColumn_(ColumnId{1}, table->ColumnDataType(ColumnId{1}), table->ColumnIsNullable(ColumnId{1}),
                             table->ColumnName(ColumnId{1})))};
  const auto aggregate = std::make_shared<AggregateHashOperator>(table_string_agg_, aggregate_expressions,
                                                                 std::vector<ColumnId>{ColumnId{0}});

  EXPECT_THROW(aggregate->Execute(operator_execution_context_), std::logic_error);
}

}  // namespace skyrise
