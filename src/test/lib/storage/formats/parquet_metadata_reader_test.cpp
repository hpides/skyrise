#include "storage/formats/parquet_metadata_reader.hpp"

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/backend/test_storage.hpp"

namespace skyrise {

class ParquetMetadataReaderTest : public ::testing::Test {
 protected:
  void SetUp() override { test_storage_ = std::make_shared<TestStorage>(); };

  std::shared_ptr<TestStorage> test_storage_;
};

TEST_F(ParquetMetadataReaderTest, CalculatePageOffsets) {
  ParquetFormatReaderOptions options;

  std::shared_ptr<ObjectReader> object_reader = test_storage_->OpenForReading("parquet/partitioned_int_string.parquet");
  const auto object_size = object_reader->GetStatus().GetSize();

  EXPECT_NO_THROW(ParquetFormatMetadataReader::CalculatePageOffsets(object_reader, object_size, options));

  const size_t small_footer_length = 10;
  EXPECT_NO_THROW(
      ParquetFormatMetadataReader::CalculatePageOffsets(object_reader, object_size, options, small_footer_length));

  const size_t too_small_footer_length = 7;
  EXPECT_THROW(
      ParquetFormatMetadataReader::CalculatePageOffsets(object_reader, object_size, options, too_small_footer_length),
      std::logic_error);
}

}  // namespace skyrise
