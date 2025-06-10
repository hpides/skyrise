#include "storage/formats/orc_reader.hpp"

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/backend/test_storage.hpp"
#include "storage/table/value_segment.hpp"
#include "testing/load_table.hpp"

namespace skyrise {

class OrcFormatReaderTest : public ::testing::Test {
 protected:
  void SetUp() override { test_storage_ = std::make_shared<TestStorage>(); };

  std::shared_ptr<TestStorage> test_storage_;
};

TEST_F(OrcFormatReaderTest, ReadAllPartitions) {
  const OrcFormatReaderOptions orc_options;

  auto orc_reader = BuildFormatReader<OrcFormatReader>(test_storage_, "orc/partitioned_int_string.orc", orc_options);

  const auto chunk = orc_reader->Next();
  EXPECT_EQ(50, chunk->GetSegment(ColumnId(0))->Size());
}

}  // namespace skyrise
