#include <gtest/gtest.h>

#include "../backend/mock_storage.hpp"
#include "storage/formats/csv.hpp"
#include "storage/table/writer.hpp"
#include "storage/types/table_column_definition.hpp"
#include "storage/types/value_segment.hpp"

namespace skyrise {

class TableWriterTest : public ::testing::Test {
 protected:
  static constexpr size_t kNumRowsPerChunk = 2;
  static constexpr size_t kCSVPartLength = 13;
  static constexpr size_t kNumThreads = 3;
  static constexpr size_t kNumChunksPerThread = 100;
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
    csv_factory_ = std::make_shared<FormatterFactory<CsvFormatter>>(options_);

    config_.format_factory = csv_factory_;
    config_.num_threads = 4;
    config_.queue_capacity = 4;
    config_.schema = schema_;
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
  CsvFormatterOptions options_;
  std::shared_ptr<FormatterFactory<CsvFormatter>> csv_factory_;
  TableWriterConfig config_;
};

TEST_F(TableWriterTest, WriteTable) {
  std::shared_ptr<MockStorage> storage = std::make_shared<MockStorage>();
  TableWriter writer(config_, storage);

  auto producer = [&]() {
    for (size_t i = 0; i < kNumChunksPerThread; i++) {
      writer.WriteChunk(chunk_);
    }
  };

  std::vector<std::thread> threads(kNumThreads);
  for (size_t i = 0; i < kNumThreads; i++) {
    threads[i] = std::thread(producer);
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  writer.Finalize();

  ASSERT_FALSE(writer.HasError());
  int chunks_found = 0;
  for (size_t last_id = 0;; last_id++) {
    ObjectStatus status = storage->GetStatus(config_.naming_strategy(last_id));
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

  ASSERT_EQ(chunks_found, kNumThreads * kNumChunksPerThread);
}

TEST_F(TableWriterTest, WriteTableErrorCase) {
  std::shared_ptr<MockStorage> storage = std::make_shared<MockStorage>();
  storage->SetSimulateWriteErrorAfter(10);  // The 10th ObjectWriter will cause an error
  TableWriter writer(config_, storage);

  auto producer = [&]() {
    for (size_t i = 0; i < kNumChunksPerThread; i++) {
      writer.WriteChunk(chunk_);
    }
  };

  std::vector<std::thread> threads(kNumThreads);
  for (size_t i = 0; i < kNumThreads; i++) {
    threads[i] = std::thread(producer);
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  writer.Finalize();

  ASSERT_TRUE(writer.HasError());
}

}  // namespace skyrise
