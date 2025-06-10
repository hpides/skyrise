#include "operator/projection_operator.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operator/execution_context.hpp"
#include "operator/table_wrapper.hpp"
#include "scheduler/worker/fragment_scheduler.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

/**
 * Projection mostly forwards its computations to the ExpressionEvaluator. The actual expression evaluation is not
 * tested here, but in expression_evaluator_test.cpp.
 */
class OperatorsProjectionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::vector<int> values_int = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<double> values_double = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    std::vector<std::string> values_string = {"0", "1", "2", "3", "4", "5", "6", "7,", "8", "9"};

    const auto value_segment_int = std::make_shared<ValueSegment<int>>(std::move(values_int));
    const auto value_segment_double = std::make_shared<ValueSegment<double>>(std::move(values_double));
    const auto value_segment_string = std::make_shared<ValueSegment<std::string>>(std::move(values_string));

    std::vector<std::shared_ptr<Chunk>> chunks = {
        std::make_shared<Chunk>(Segments({value_segment_int, value_segment_double, value_segment_string})),
        std::make_shared<Chunk>(Segments({value_segment_int, value_segment_double, value_segment_string}))};

    const TableColumnDefinitions definitions = {TableColumnDefinition("a", DataType::kInt, false),
                                                TableColumnDefinition("b", DataType::kDouble, false),
                                                TableColumnDefinition("c", DataType::kString, false)};
    table_ = std::make_shared<Table>(definitions, std::move(chunks));
    table_wrapper_ = std::make_shared<TableWrapper>(table_);
    // The input operator needs to be executed before the projection operator itself.
    // We do it manually here because the scheduler, which takes care of it normally, is not used here.
    table_wrapper_->Execute(nullptr);

    operator_context_ = std::make_shared<OperatorExecutionContext>(
        nullptr, nullptr, []() { return std::make_shared<FragmentScheduler>(); });
  }

  std::shared_ptr<Table> table_;
  std::shared_ptr<TableWrapper> table_wrapper_;
  std::shared_ptr<OperatorExecutionContext> operator_context_;
};

TEST_F(OperatorsProjectionTest, OperatorName) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      std::make_shared<PqpColumnExpression>(0, DataType::kInt, false, "Column1")};
  const auto projection = std::make_shared<ProjectionOperator>(table_wrapper_, expressions);
  EXPECT_EQ(projection->Name(), "Projection");
}

TEST_F(OperatorsProjectionTest, ExecuteOnAllChunks) {
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      std::make_shared<PqpColumnExpression>(2, DataType::kString, false, "Column3"),
      std::make_shared<PqpColumnExpression>(0, DataType::kInt, false, "Column1")};
  const auto projection = std::make_shared<ProjectionOperator>(table_wrapper_, expressions);
  projection->Execute(operator_context_);
  const std::shared_ptr<const skyrise::Table> output_table = projection->GetOutput();
  EXPECT_EQ(output_table->ChunkCount(), table_->ChunkCount());
  EXPECT_EQ(output_table->GetColumnCount(), expressions.size());

  const auto chunk = output_table->GetChunk(0);
  EXPECT_EQ(chunk->GetSegment(0)->GetDataType(), DataType::kString);
  EXPECT_EQ(chunk->GetSegment(1)->GetDataType(), DataType::kInt);

  const auto column_definitions = output_table->ColumnDefinitions();
  EXPECT_EQ(column_definitions.at(0).data_type, DataType::kString);
  EXPECT_EQ(column_definitions.at(1).data_type, DataType::kInt);
  EXPECT_EQ(column_definitions.at(0).name, "Column3");
  EXPECT_EQ(column_definitions.at(1).name, "Column1");
}

TEST_F(OperatorsProjectionTest, ExecuteNonPqpExpression) {
  const auto pqp_expression = std::make_shared<PqpColumnExpression>(0, DataType::kInt, false, "Column1");
  const auto literal_expression = std::make_shared<ValueExpression>(1);
  const std::vector<std::shared_ptr<AbstractExpression>> expressions = {
      std::make_shared<ArithmeticExpression>(ArithmeticOperator::kAddition, pqp_expression, literal_expression)};
  const auto projection = std::make_shared<ProjectionOperator>(table_wrapper_, expressions);
  projection->Execute(operator_context_);
  const std::shared_ptr<const skyrise::Table> output_table = projection->GetOutput();

  EXPECT_EQ(output_table->ChunkCount(), table_->ChunkCount());
  EXPECT_EQ(output_table->GetColumnCount(), 1);

  const auto chunk = output_table->GetChunk(0);
  EXPECT_EQ(chunk->GetSegment(0)->GetDataType(), DataType::kInt);

  const auto* output_segment = dynamic_cast<ValueSegment<int32_t>*>(output_table->GetChunk(0)->GetSegment(0).get());
  const auto* segment = dynamic_cast<ValueSegment<int32_t>*>(table_->GetChunk(0)->GetSegment(0).get());

  for (size_t row_id = 0; row_id < segment->Size(); ++row_id) {
    EXPECT_EQ(segment->Values()[row_id] + 1, output_segment->Values()[row_id]);
  }
}

}  // namespace skyrise
