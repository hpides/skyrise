#include "statistics/manifest_reader.hpp"

#include "manifest_shared.hpp"
#include "statistics/manifest_writer.hpp"

namespace skyrise {

class ManifestReaderTest : public ManifestTest {
 protected:
  void SetUp() override {
    ManifestTest::SetUp();
    WriteMockPartition(1);
  };

  void WriteMockPartition(size_t num_fragments) {
    manifest_writer_ = std::make_shared<ManifestWriter>(storage_->OpenForWriting(kMetadataFile));
    manifest_writer_->SetTablePrefix(kTablePrefix);
    for (size_t i = 0; i < num_fragments; i++) {
      manifest_writer_->WritePartition(statistics_);
    }
    manifest_writer_->Close();
  }

  inline static const std::string kMetadataFile{"manifest.orc"};
  inline static const std::string kTablePrefix = "someprefix";
  std::shared_ptr<ManifestWriter> manifest_writer_;
};

TEST_F(ManifestReaderTest, ReadOriginalSchema) {
  ManifestReader reader(storage_->OpenForReading(kMetadataFile));

  auto original_schema = reader.GetOriginalSchema();
  EXPECT_EQ(original_schema->size(), schema_->size());

  EXPECT_EQ(*original_schema, *schema_);
  EXPECT_NE(original_schema.get(), schema_.get());
}

TEST_F(ManifestReaderTest, ReadSchema) {
  ManifestReader reader(storage_->OpenForReading(kMetadataFile));

  EXPECT_EQ(*reader.GetOriginalSchema(), *statistics_.schema);
}

TEST_F(ManifestReaderTest, ReadTablePrefix) {
  ManifestReader reader(storage_->OpenForReading(kMetadataFile));

  EXPECT_EQ(reader.GetTablePrefix(), kTablePrefix);
}

TEST_F(ManifestReaderTest, ReadManifestVersion) {
  ManifestReader reader(storage_->OpenForReading(kMetadataFile));

  EXPECT_EQ(reader.GetManifestVersion(), manifest_writer_->GetManifestVersion());
}

TEST_F(ManifestReaderTest, ReadObjectStatistic) {
  ManifestReader reader(storage_->OpenForReading(kMetadataFile));

  EXPECT_EQ(reader.GetNumberOfPartitions(), 1);
  EXPECT_TRUE(reader.HasNextPartition());
  ObjectStatistics result = reader.ReadNextPartition();
  EXPECT_FALSE(reader.HasNextPartition());

  EXPECT_EQ(result.object_identifier, kDataFile);
  EXPECT_EQ(result.etag, statistics_.etag);
  EXPECT_EQ(result.last_modified, statistics_.last_modified);
  EXPECT_EQ(result.filesize, statistics_.filesize);
  EXPECT_EQ(result.num_rows, statistics_.num_rows);
  EXPECT_EQ(result.null_count, statistics_.null_count);
  EXPECT_EQ(result.minmax, statistics_.minmax);
}

TEST_F(ManifestReaderTest, MultipleReadBatchesRequired) {
  size_t num_partitions = 100000;
  WriteMockPartition(num_partitions);

  ManifestReader reader(storage_->OpenForReading(kMetadataFile));

  EXPECT_EQ(reader.GetNumberOfPartitions(), num_partitions);
  for (size_t i = 0; i < num_partitions; i++) {
    EXPECT_TRUE(reader.HasNextPartition());
    reader.ReadNextPartition();
  }
  EXPECT_FALSE(reader.HasNextPartition());
}

}  // namespace skyrise
