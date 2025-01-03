#include "storage/formats/parquet_reader.hpp"

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/backend/testdata_storage.hpp"
#include "storage/table/value_segment.hpp"
#include "testing/load_table.hpp"

namespace skyrise {

class ParquetFormatReaderTest : public ::testing::Test {
 protected:
  void SetUp() override { test_data_storage_ = std::make_shared<TestdataStorage>(); };

  std::shared_ptr<TestdataStorage> test_data_storage_;
};

TEST_F(ParquetFormatReaderTest, ArrowPredicatePushdown) {
  ParquetFormatReaderOptions parquet_options;
  parquet_options.arrow_expression = arrow::compute::less(arrow::compute::field_ref("a"), arrow::compute::literal(5));

  auto parquet_reader = BuildFormatReader<ParquetFormatReader>(
      test_data_storage_, "parquet/partitioned_int_string.parquet", parquet_options);

  const auto chunk = parquet_reader->Next();
  EXPECT_EQ(5, chunk->GetSegment(ColumnId(0))->Size());
}

TEST_F(ParquetFormatReaderTest, ProjectionPushdown) {
  const auto parquet_options = ParquetFormatReaderOptions{};

  // Test without projection
  auto parquet_reader = BuildFormatReader<ParquetFormatReader>(
      test_data_storage_, "parquet/partitioned_int_string.parquet", parquet_options);

  const auto chunk = parquet_reader->Next();
  EXPECT_EQ(2, chunk->GetColumnCount());

  // Test with projection
  auto parquet_options_projection = ParquetFormatReaderOptions{};
  parquet_options_projection.include_columns = std::vector<ColumnId>{0};
  auto parquet_reader_projection = BuildFormatReader<ParquetFormatReader>(
      test_data_storage_, "parquet/partitioned_int_string.parquet", parquet_options_projection);

  const auto chunk_projection = parquet_reader_projection->Next();
  EXPECT_EQ(1, chunk_projection->GetColumnCount());
}

TEST_F(ParquetFormatReaderTest, ReadAllPartitions) {
  const ParquetFormatReaderOptions parquet_options;

  auto parquet_reader = BuildFormatReader<ParquetFormatReader>(
      test_data_storage_, "parquet/partitioned_int_string.parquet", parquet_options);

  const auto chunk = parquet_reader->Next();
  EXPECT_EQ(5, chunk->GetSegment(ColumnId(0))->Size());
  EXPECT_TRUE(parquet_reader->HasNext());
}

TEST_F(ParquetFormatReaderTest, ReadFirstPartition) {
  ParquetFormatReaderOptions parquet_options;
  parquet_options.row_group_ids = {0};

  auto parquet_reader = BuildFormatReader<ParquetFormatReader>(
      test_data_storage_, "parquet/partitioned_int_string.parquet", parquet_options);

  const auto chunk = parquet_reader->Next();
  EXPECT_EQ(5, chunk->GetSegment(ColumnId(0))->Size());
  EXPECT_EQ(0, std::static_pointer_cast<ValueSegment<int>>(chunk->GetSegment(ColumnId(0)))->GetTypedValue(0));
  EXPECT_FALSE(parquet_reader->HasNext());
}

TEST_F(ParquetFormatReaderTest, ReadLastPartition) {
  ParquetFormatReaderOptions parquet_options;
  parquet_options.row_group_ids = {1};

  auto parquet_reader = BuildFormatReader<ParquetFormatReader>(
      test_data_storage_, "parquet/partitioned_int_string.parquet", parquet_options);

  const auto chunk = parquet_reader->Next();
  EXPECT_EQ(5, chunk->GetSegment(ColumnId(0))->Size());
  EXPECT_EQ(5, std::static_pointer_cast<ValueSegment<int>>(chunk->GetSegment(ColumnId(0)))->GetTypedValue(0));
  EXPECT_FALSE(parquet_reader->HasNext());
}

TEST_F(ParquetFormatReaderTest, ReadInvalidPartition) {
  ParquetFormatReaderOptions parquet_options;
  parquet_options.row_group_ids = {1};
  const std::string expected_error_message =
      "Index error: ParquetFileFragment references row group 1 but <Buffer> only has 1 row groups";

  auto parquet_reader =
      BuildFormatReader<ParquetFormatReader>(test_data_storage_, "parquet/with_types.parquet", parquet_options);

  EXPECT_TRUE(parquet_reader->HasError());
  const std::string error_message = parquet_reader->GetError().GetMessage();
  // TODO(tobodner): Move Assert into skyrise namespace and use ::testing::HasSubstr() from gmock/gmock-matchers.h
  EXPECT_TRUE(0 == error_message.compare(error_message.length() - expected_error_message.length(),
                                         expected_error_message.length(), expected_error_message));
}

}  // namespace skyrise
