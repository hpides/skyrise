#include "testing/load_table.hpp"

#include <gtest/gtest.h>

#include "storage/backend/test_storage.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/formats/parquet_reader.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"
#include "testing/testing_assert.hpp"

namespace skyrise {

class LoadTableTest : public ::testing::Test {
  void SetUp() override {
    storage_ = std::make_shared<TestStorage>();

    TableColumnDefinitions schema;
    schema.emplace_back("a", DataType::kInt, false);
    schema.emplace_back("b", DataType::kFloat, false);

    std::vector<int32_t> int_values{12345, 123, 1234};
    std::vector<float> float_values{458.7, 456.7, 457.7};

    Segments segments;
    segments.push_back(std::make_shared<ValueSegment<int32_t>>(std::move(int_values)));
    segments.push_back(std::make_shared<ValueSegment<float>>(std::move(float_values)));

    std::vector<std::shared_ptr<Chunk>> chunks;
    chunks.push_back(std::make_shared<Chunk>(segments));

    table_float_int_ = std::make_shared<Table>(schema, std::move(chunks));
  }

 protected:
  std::shared_ptr<TestStorage> storage_;
  std::shared_ptr<Table> table_float_int_;
};

TEST_F(LoadTableTest, LoadTblTable) {
  CsvFormatReaderOptions configuration;
  configuration.guess_delimiter = true;
  configuration.has_header = true;
  configuration.has_types = true;

  const auto table = LoadTable<CsvFormatReader>("tbl/int_float.tbl", storage_, configuration);

  EXPECT_TABLE_EQ_UNORDERED(table, std::const_pointer_cast<Table>(table_float_int_));
}

TEST_F(LoadTableTest, LoadCsvTable) {
  const auto table = LoadTable<CsvFormatReader>("csv/with_types.csv", storage_);

  EXPECT_EQ(table->RowCount(), 1);
  EXPECT_EQ(table->GetColumnCount(), 5);
}

TEST_F(LoadTableTest, LoadOrcTable) {
  const auto table = LoadTable<OrcFormatReader>("orc/with_types.orc", storage_);

  EXPECT_EQ(table->RowCount(), 1);
  EXPECT_EQ(table->GetColumnCount(), 5);
}

TEST_F(LoadTableTest, LoadParquetTable) {
  const auto table = LoadTable<ParquetFormatReader>("parquet/with_types.parquet", storage_);

  EXPECT_EQ(table->RowCount(), 1);
  EXPECT_EQ(table->GetColumnCount(), 5);
}

TEST_F(LoadTableTest, CsvFileNotFound) { EXPECT_ANY_THROW(LoadTable<CsvFormatReader>("csv/error_test.csv", storage_)); }

TEST_F(LoadTableTest, OrcFileNotFound) { EXPECT_ANY_THROW(LoadTable<OrcFormatReader>("orc/error_test.orc", storage_)); }

TEST_F(LoadTableTest, ParquetFileNotFound) {
  EXPECT_ANY_THROW(LoadTable<ParquetFormatReader>("parquet/error_test.parquet", storage_));
}

}  // namespace skyrise
