#include "statistics/manifest_writer.hpp"

#include "manifest_shared.hpp"
#include "statistics/manifest_reader.hpp"

namespace skyrise {

class ManifestWriterTest : public ManifestTest {
 protected:
  void SetUp() override { ManifestTest::SetUp(); };

  inline static const std::string kTestFile{"test.orc"};
};

TEST_F(ManifestWriterTest, WriteManifestWithSinglePartition) {
  ManifestWriter manifest_writer(storage_->OpenForWriting(kTestFile));
  manifest_writer.WritePartition(statistics_);
  manifest_writer.Close();

  EXPECT_FALSE(manifest_writer.GetError());

  ManifestReader reader(storage_->OpenForReading(kTestFile));

  EXPECT_EQ(reader.GetNumberOfPartitions(), 1);

  // Expect to have 3 stats (min, max, nullcount) for each of the 5 columns.
  auto partition = reader.ReadNextPartition();
  EXPECT_EQ(partition.minmax.size(), 5);
  EXPECT_EQ(partition.null_count.size(), 5);
  // Expect to have the original number of columns in the metadata.
  EXPECT_EQ(reader.GetOriginalSchema()->size(), 5);

  EXPECT_FALSE(reader.HasNextPartition());

  TableColumnDefinitions original_schema = *reader.GetOriginalSchema();

  EXPECT_EQ(original_schema.size(), 5);
  EXPECT_EQ(original_schema[0].name, "id");
  EXPECT_EQ(original_schema[0].data_type, DataType::kLong);
  EXPECT_EQ(original_schema[0].nullable, false);
}

TEST_F(ManifestWriterTest, WriteManifestWithMultiplePartitions) {
  ManifestWriter manifest_writer(storage_->OpenForWriting(kTestFile));
  manifest_writer.WritePartition(statistics_);
  manifest_writer.WritePartition(statistics_);
  manifest_writer.WritePartition(statistics_);
  manifest_writer.Close();

  EXPECT_FALSE(manifest_writer.GetError());

  ManifestReader reader(storage_->OpenForReading(kTestFile));
  EXPECT_EQ(reader.GetNumberOfPartitions(), 3);
}

}  // namespace skyrise
