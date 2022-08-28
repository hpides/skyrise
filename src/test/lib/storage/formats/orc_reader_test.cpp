#include "storage/formats/orc_reader.hpp"

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/backend/testdata_storage.hpp"
#include "storage/formats/orc_writer.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {
namespace {

std::shared_ptr<Chunk> CreateChunkWithConstantValues(size_t constant_value, size_t segment_capacity) {
  auto column = std::make_shared<ValueSegment<int32_t>>(false, segment_capacity);
  column->Values().insert(column->Values().begin(), segment_capacity, constant_value);
  Segments segments;
  segments.emplace_back(std::move(column));
  return std::make_shared<Chunk>(segments);
}

std::shared_ptr<Chunk> CreateEmptyChunk() {
  return std::make_shared<Chunk>(Segments{std::make_shared<ValueSegment<int32_t>>(false, 1)});
}

template <typename SegmentValueType>
void CheckSegment(const std::shared_ptr<AbstractSegment>& segment,
                  const std::function<bool(SegmentValueType value)>& test_function) {
  auto value_segment = std::dynamic_pointer_cast<ValueSegment<SegmentValueType>>(segment);
  bool all_values_as_expected =
      std::all_of(value_segment->Values().begin(), value_segment->Values().end(), test_function);
  ASSERT_TRUE(all_values_as_expected);
}

template <typename SegmentValueType>
size_t ReadChunksAndCheckFirstSegment(OrcFormatReader* reader,
                                      const std::function<bool(SegmentValueType value)>& test_function) {
  size_t num_rows = 0;
  while (reader->HasNext()) {
    auto chunk = reader->Next();
    CheckSegment<SegmentValueType>(chunk->GetSegment(0), test_function);
    num_rows += chunk->Size();
  }
  return num_rows;
}

}  // namespace

class OrcFormatReaderTest : public ::testing::Test {
 protected:
  void CreatePartitionedObject(bool first_partition_is_empty = false) {
    TableColumnDefinitions schema;
    schema.emplace_back("numbers", DataType::kInt, false);

    std::shared_ptr<Chunk> partition1 =
        first_partition_is_empty ? CreateEmptyChunk() : CreateChunkWithConstantValues(1, kPartitionSegmentCapacity);
    std::shared_ptr<Chunk> partition2 = CreateChunkWithConstantValues(2, kPartitionSegmentCapacity);
    std::shared_ptr<Chunk> partition3 = CreateChunkWithConstantValues(3, kPartitionSegmentCapacity);

    auto object_writer = storage_.OpenForWriting(kPartitionedObjectName);
    OrcFormatWriterOptions options;
    options.save_chunk_offsets = true;
    OrcFormatWriter orc_writer(options);
    orc_writer.SetOutputHandler(
        [&object_writer](const char* data, size_t length) { object_writer->Write(data, length); });
    orc_writer.Initialize(schema);
    orc_writer.ProcessChunk(partition1);
    orc_writer.ProcessChunk(partition2);
    orc_writer.ProcessChunk(partition3);
    orc_writer.Finalize();
    object_writer->Close();
  }

  void CreatePartitionedObjectWithEmptyFirstPartition() { CreatePartitionedObject(true); }

  MockStorage storage_;
  TestdataStorage testdata_;
  inline static const std::string kOrcObjectName{"table.orc"};
  static constexpr size_t kPartitionSegmentCapacity = 10;
  inline static const std::string kPartitionedObjectName{"partioned.orc"};
};

TEST_F(OrcFormatReaderTest, ReadSinglePartition) {
  CreatePartitionedObject();

  OrcFormatReaderOptions options;
  options.select_partition_range = std::pair<size_t, size_t>(1, 1);
  OrcFormatReader reader(storage_.OpenForReading(kPartitionedObjectName), options);
  size_t num_rows = ReadChunksAndCheckFirstSegment<int32_t>(&reader, [](int32_t v) { return v == 2; });

  ASSERT_EQ(num_rows, kPartitionSegmentCapacity);
}

TEST_F(OrcFormatReaderTest, ReadFirstTwoPartition) {
  CreatePartitionedObject();

  OrcFormatReaderOptions options;
  options.select_partition_range = std::pair<size_t, size_t>(0, 1);
  OrcFormatReader reader(storage_.OpenForReading(kPartitionedObjectName), options);
  size_t num_rows = ReadChunksAndCheckFirstSegment<int32_t>(&reader, [](int32_t v) { return v == 1 || v == 2; });

  ASSERT_EQ(num_rows, 2 * kPartitionSegmentCapacity);
}

TEST_F(OrcFormatReaderTest, ReadLastTwoPartition) {
  CreatePartitionedObject();

  OrcFormatReaderOptions options;
  options.select_partition_range = std::pair<size_t, size_t>(1, 2);
  OrcFormatReader reader(storage_.OpenForReading(kPartitionedObjectName), options);
  size_t num_rows = ReadChunksAndCheckFirstSegment<int32_t>(&reader, [](int32_t v) { return v == 2 || v == 3; });

  ASSERT_EQ(num_rows, 2 * kPartitionSegmentCapacity);
}

TEST_F(OrcFormatReaderTest, ReadEmptyPartition) {
  CreatePartitionedObjectWithEmptyFirstPartition();

  OrcFormatReaderOptions options;
  options.select_partition_range = std::pair<size_t, size_t>(0, 0);
  OrcFormatReader reader(storage_.OpenForReading(kPartitionedObjectName), options);
  EXPECT_FALSE(reader.HasError());
  EXPECT_FALSE(reader.HasNext());
}

TEST_F(OrcFormatReaderTest, ReadInvalidPartition) {
  CreatePartitionedObject();

  OrcFormatReaderOptions options;
  options.select_partition_range = std::pair<size_t, size_t>(1, 8);
  OrcFormatReader reader(storage_.OpenForReading(kPartitionedObjectName), options);
  EXPECT_TRUE(reader.HasError());
}

TEST_F(OrcFormatReaderTest, ReadSingleRow) {
  // This test of course works with non partitioned ORC files too.
  CreatePartitionedObject();

  OrcFormatReaderOptions options;
  options.select_row_range = std::pair<size_t, size_t>(0, 0);
  OrcFormatReader reader(storage_.OpenForReading(kPartitionedObjectName), options);
  size_t num_rows = 0;
  while (reader.HasNext()) {
    auto chunk = reader.Next();
    auto value_segment = std::dynamic_pointer_cast<ValueSegment<int32_t>>(chunk->GetSegment(0));
    bool all_values_as_expected =
        std::all_of(value_segment->Values().begin(), value_segment->Values().end(), [](int32_t v) { return v == 1; });
    ASSERT_TRUE(all_values_as_expected);
    num_rows += chunk->Size();
  }

  ASSERT_EQ(num_rows, 1);
}

}  // namespace skyrise
