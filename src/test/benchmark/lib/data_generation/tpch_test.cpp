#include <gtest/gtest.h>

#include "data_generation/tpch/tpch_generator.hpp"
#include "lib/storage/backend/mock_storage.hpp"
#include "storage/formats/csv_writer.hpp"
#include "storage/table/chunk_writer.hpp"

namespace skyrise {

class TpchDataGeneratorTest : public ::testing::Test {
 protected:
  static constexpr float kScaleFactor = 0.01;
  void SetUp() override {
    // Define output format
    CsvFormatWriterOptions csv_options;
    csv_options.field_separator = ',';
    csv_options.include_headers = true;
    csv_factory_ = std::make_shared<FormatterFactory<CsvFormatWriter>>(csv_options);

    // Define storage backend
    storage_ = std::make_shared<MockStorage>();

    // Define how to obtain a table writer for a given table
    get_chunk_writer_ = [this](const std::string& name,
                               const TableColumnDefinitions& schema) -> std::shared_ptr<PartitionedChunkWriter> {
      PartitionedChunkWriterConfig writer_config;
      writer_config.naming_strategy = [name](size_t /*part*/) { return name + ".csv"; };
      writer_config.format_factory = csv_factory_;
      writer_config.num_threads = 1;
      writer_config.queue_capacity = 1;
      auto chunk_writer = std::make_shared<PartitionedChunkWriter>(writer_config, storage_);
      chunk_writer->Initialize(schema);
      return chunk_writer;
    };
  }

  std::shared_ptr<MockStorage> storage_;
  std::shared_ptr<AbstractFormatWriterFactory> csv_factory_;
  PartitionedChunkWriterFactory get_chunk_writer_;
};

TEST_F(TpchDataGeneratorTest, GenerateRegionTable) {
  // We do not have a region table at the beginning
  ObjectStatus status = storage_->GetStatus("region.csv");
  ASSERT_TRUE(status.GetError());

  // Now generate table
  TPCHGenerator generator(get_chunk_writer_, kScaleFactor);
  generator.DisableAllTables();
  generator.EnableTable(TpchTable::kRegion);
  generator.Generate();

  // Now we should see the file
  status = storage_->GetStatus("region.csv");
  ASSERT_FALSE(status.GetError());
  ASSERT_GT(status.GetSize(), 0);

  // Check contents of file
  std::string content;
  storage_->OpenForReading("region.csv")
      ->Read(0, ObjectReader::kLastByteInFile,
             [&content](const char* data, size_t length) { content.append(data, length); });

  ASSERT_TRUE(content.find("r_regionkey,r_name,r_comment") != content.npos);
  ASSERT_TRUE(content.find("AFRICA") != content.npos);
  ASSERT_TRUE(content.find("EUROPE") != content.npos);
  ASSERT_TRUE(content.find("MIDDLE EAST") != content.npos);
}

TEST_F(TpchDataGeneratorTest, GenerateAlmostAllTables) {
  const std::vector<std::string> tables = {"partsupp.csv", "supplier.csv", "customer.csv",
                                           "orders.csv",   "nation.csv",   "region.csv"};

  TPCHGenerator generator(get_chunk_writer_, kScaleFactor);
  generator.EnableAllTables();
  generator.DisableTable(TpchTable::kLineItem);
  generator.DisableTable(TpchTable::kPart);
  generator.Generate();

  for (const auto& table_name : tables) {
    ObjectStatus info = storage_->GetStatus(table_name);
    ASSERT_FALSE(info.GetError());
    ASSERT_GT(info.GetSize(), 0);
  }

  ASSERT_TRUE(storage_->GetStatus("lineitem.csv").GetError());
  ASSERT_TRUE(storage_->GetStatus("part.csv").GetError());
}

TEST_F(TpchDataGeneratorTest, GenerateAllTables) {
  const std::vector<std::string> tables = {"part.csv",   "partsupp.csv", "supplier.csv", "customer.csv",
                                           "orders.csv", "nation.csv",   "region.csv",   "lineitem.csv"};

  TPCHGenerator generator(get_chunk_writer_, kScaleFactor);
  generator.EnableAllTables();
  generator.Generate();

  for (const auto& table_name : tables) {
    ObjectStatus info = storage_->GetStatus(table_name);
    ASSERT_FALSE(info.GetError());
    ASSERT_GT(info.GetSize(), 0);
  }
}

}  // namespace skyrise
