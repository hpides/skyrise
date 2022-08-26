#include "storage/formats/parquet_reader.hpp"

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/backend/testdata_storage.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class ParquetFormatReaderTest : public ::testing::Test {
 protected:
  void SetUp() override { test_data_storage_ = std::make_shared<TestdataStorage>(); };

  std::shared_ptr<TestdataStorage> test_data_storage_;
};

TEST_F(ParquetFormatReaderTest, ArrowPredicatePushdown) {
  ParquetFormatReaderOptions parquet_options;
  parquet_options.arrow_expression = arrow::compute::less(arrow::compute::field_ref("a"), arrow::compute::literal(5));

  ParquetFormatReader parquet_reader(test_data_storage_->OpenForReading("parquet/partitioned_int_string.parquet"),
                                     parquet_options);

  const auto chunk = parquet_reader.Next();
  EXPECT_EQ(5, chunk->GetSegment(ColumnId(0))->Size());
}

}  // namespace skyrise
