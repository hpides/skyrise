#include "storage/table/chunk_writer.hpp"

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/formats/csv_writer.hpp"
#include "storage/table/table_column_definition.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class PartitionedChunkWriterTest : public ::testing::Test {
 protected:
  static constexpr size_t kNumRowsPerChunk = 2;
  static constexpr size_t kCSVPartLength = 13;
  static constexpr size_t kNumSequentialTasks = 3;
  static constexpr size_t kNumChunksPerTask = 100;
  static constexpr size_t kNumChunksPerObject = 2;

  void SetUp() override {
    schema_.emplace_back("A", DataType::kInt, false);
    schema_.emplace_back("B", DataType::kString, false);

    value_segment_int_ = std::make_shared<ValueSegment<int>>();
    value_segment_int_->Append(1);
    value_segment_int_->Append(3);
    value_segment_str_ = std::make_shared<ValueSegment<std::string>>();
    value_segment_str_->Append(std::string("two"));
    value_segment_str_->Append(std::string("four"));

    chunk_ = std::make_shared<Chunk>(Segments({value_segment_int_, value_segment_str_}));

    options_.include_headers = false;
    options_.field_separator = ",";
    options_.record_separator = "\n";
    csv_factory_ = std::make_shared<FormatterFactory<CsvFormatWriter>>(options_);

    config_.format_factory = csv_factory_;
    config_.split_rows = kNumRowsPerChunk * kNumChunksPerObject;
    config_.naming_strategy = [](size_t part) -> std::string {
      auto ss = std::stringstream();
      ss << "part" << part << ".csv";
      return ss.str();
    };

    /*
    The generated CSV file will look like this:
      1,two\n
      3,four\n
      ------------
      = 13 byte
    */
  }

  std::shared_ptr<Chunk> chunk_;
  std::shared_ptr<BaseValueSegment> value_segment_int_;
  std::shared_ptr<BaseValueSegment> value_segment_str_;
  TableColumnDefinitions schema_;
  CsvFormatWriterOptions options_;
  std::shared_ptr<FormatterFactory<CsvFormatWriter>> csv_factory_;
  PartitionedChunkWriterConfig config_;
};

TEST_F(PartitionedChunkWriterTest, WriteTable) {
  const std::shared_ptr<MockStorage> storage = std::make_shared<MockStorage>();
  PartitionedChunkWriter writer(config_, storage);
  writer.Initialize(schema_);

  auto producer = [&]() {
    for (size_t i = 0; i < kNumChunksPerTask; ++i) {
      writer.ProcessChunk(chunk_);
    }
  };

  for (size_t i = 0; i < kNumSequentialTasks; ++i) {
    producer();
  }

  writer.Finalize();

  ASSERT_FALSE(writer.HasError());
  int chunks_found = 0;
  for (size_t last_id = 0;; ++last_id) {
    const ObjectStatus status = storage->GetStatus(config_.naming_strategy(last_id));
    if (status.GetError()) {
      break;
    }
    if (status.GetSize() == kCSVPartLength) {
      chunks_found += 1;
    } else if (status.GetSize() == kCSVPartLength * 2) {
      chunks_found += 2;
    } else {
      ASSERT_TRUE(false);
    }
  }

  ASSERT_EQ(chunks_found, kNumSequentialTasks * kNumChunksPerTask);
}

TEST_F(PartitionedChunkWriterTest, WriteTableErrorCase) {
  const std::shared_ptr<MockStorage> storage = std::make_shared<MockStorage>();
  storage->SetSimulateWriteErrorAfter(10);  // The 10th ObjectWriter will cause an error
  PartitionedChunkWriter writer(config_, storage);
  writer.Initialize(schema_);

  auto producer = [&]() {
    for (size_t i = 0; i < kNumChunksPerTask; ++i) {
      writer.ProcessChunk(chunk_);
    }
  };

  for (size_t i = 0; i < kNumSequentialTasks; ++i) {
    producer();
  }

  writer.Finalize();

  ASSERT_TRUE(writer.HasError());
}

}  // namespace skyrise
