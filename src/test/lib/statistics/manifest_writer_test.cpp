#include "statistics/manifest_writer.hpp"

#include <gtest/gtest.h>

#include "manifest_shared.hpp"

namespace skyrise {

class ManifestWriterTest : public ManifestTest {
 protected:
  void SetUp() override { ManifestTest::SetUp(); };

  static constexpr auto kTestFile = "test.orc";
};

TEST_F(ManifestWriterTest, WriteManifestWithSinglePartition) {
  ManifestWriter manifest_writer(storage_->OpenForWriting(kTestFile));
  manifest_writer.WritePartition(statistics_);
  manifest_writer.Close();

  EXPECT_FALSE(manifest_writer.GetError());

  auto input_stream = std::make_unique<detail::OrcInputStream>(storage_, kTestFile);

  orc::ReaderOptions options;
  auto orc_reader = orc::createReader(std::move(input_stream), options);
  EXPECT_EQ(orc_reader->getNumberOfRows(), 1);
  EXPECT_EQ(orc_reader->getType().getKind(), orc::STRUCT);

  // Metadata (6) per file + 3 stats (min, max, nullcount) per column (16 columns).
  EXPECT_EQ(orc_reader->getType().getSubtypeCount(), 6 + 5 * 3);
  // Expect to have the original number of columns in the metadata.
  EXPECT_EQ(orc_reader->getMetadataValue("columns"), std::to_string(5));

  TableColumnDefinitions original_schema;
  auto buffer = std::make_shared<std::stringstream>(orc_reader->getMetadataValue("schema"));
  BinarySerializationStream serializer(buffer);
  serializer >> original_schema;

  EXPECT_EQ(original_schema.size(), 5);
  EXPECT_EQ(original_schema[0].name, "id");
  EXPECT_EQ(original_schema[0].data_type, DataType::kLong);
  EXPECT_EQ(original_schema[0].nullable, false);
}

TEST_F(ManifestWriterTest, WriteManifestWithMultiplePartition) {
  ManifestWriter manifest_writer(storage_->OpenForWriting(kTestFile));
  manifest_writer.WritePartition(statistics_);
  manifest_writer.WritePartition(statistics_);
  manifest_writer.WritePartition(statistics_);
  manifest_writer.Close();

  EXPECT_FALSE(manifest_writer.GetError());

  auto input_stream = std::make_unique<detail::OrcInputStream>(storage_, kTestFile);

  orc::ReaderOptions options;
  auto orc_reader = orc::createReader(std::move(input_stream), options);
  EXPECT_EQ(orc_reader->getNumberOfRows(), 3);
}

}  // namespace skyrise
