#include "statistics/manifest_merger.hpp"

#include "manifest_shared.hpp"
#include "statistics/manifest_reader.hpp"

namespace skyrise {

class ManifestMergerTest : public ManifestTest {
 protected:
  void WriteMockManifest(const std::string& file_name, ObjectStatistics statistics, size_t num_partitions) {
    auto writer = storage_->OpenForWriting(file_name);
    ManifestWriter manifest_writer(std::move(writer));

    statistics.object_identifier = file_name;

    for (size_t i = 0; i < num_partitions; ++i) {
      manifest_writer.WritePartition(statistics);
    }

    manifest_writer.Close();
  }

  void WriteMockManifest(const std::string& file_name, ObjectStatistics statistics) {
    WriteMockManifest(file_name, std::move(statistics), 1);
  }
};

TEST_F(ManifestMergerTest, MergeManifestFiles) {
  WriteMockManifest("mockA.orc", statistics_, 1);
  WriteMockManifest("mockB.orc", statistics_, 2);
  WriteMockManifest("mockC.orc", statistics_, 3);

  ManifestMerger merger(storage_);
  const bool is_success = merger.Merge({"mockA.orc", "mockB.orc", "mockC.orc"}, "mockD.orc");
  EXPECT_TRUE(is_success);
  EXPECT_FALSE(merger.GetError());

  ManifestReader reader(storage_->OpenForReading("mockD.orc"));

  EXPECT_EQ(*reader.GetOriginalSchema(), *schema_);
  EXPECT_EQ(reader.GetNumberOfPartitions(), 6);
  EXPECT_EQ(reader.ReadNextPartition().object_identifier, "mockA.orc");
  EXPECT_EQ(reader.ReadNextPartition().object_identifier, "mockB.orc");
  EXPECT_EQ(reader.ReadNextPartition().object_identifier, "mockB.orc");
  EXPECT_EQ(reader.ReadNextPartition().object_identifier, "mockC.orc");
  EXPECT_EQ(reader.ReadNextPartition().object_identifier, "mockC.orc");
  EXPECT_EQ(reader.ReadNextPartition().object_identifier, "mockC.orc");
}

TEST_F(ManifestMergerTest, NoMergeDueToSchemaMissmatch) {
  WriteMockManifest("mockA.orc", statistics_);

  ObjectStatistics different_statistic;
  const auto schema = std::make_shared<TableColumnDefinitions>();
  different_statistic.schema = schema;
  WriteMockManifest("mockB.orc", different_statistic);

  ManifestMerger merger(storage_);
  const bool is_success = merger.Merge({"mockA.orc", "mockB.orc"}, "mockD.orc");
  EXPECT_FALSE(is_success);
}

}  // namespace skyrise
