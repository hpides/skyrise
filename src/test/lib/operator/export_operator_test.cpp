#include "operator/export_operator.hpp"

#include <string_view>

#include <gtest/gtest.h>

#include "operator/table_wrapper.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/backend/mock_storage.hpp"
#include "storage/formats/mock_chunk_reader.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class ExportOperatorTest : public ::testing::Test {};

std::shared_ptr<const Table> CreateTableContainingValue(size_t num_chunks, ChunkOffset num_rows_per_chunk,
                                                        int32_t value) {
  MockChunkReaderConfiguration config;
  config.num_chunks = num_chunks;
  config.generators.emplace_back([num_rows_per_chunk, value]() {
    return std::make_shared<ValueSegment<int32_t>>(std::vector<int32_t>(num_rows_per_chunk, value));
  });
  MockChunkReader reader(nullptr, config);
  std::vector<std::shared_ptr<Chunk>> chunks;
  while (reader.HasNext()) {
    chunks.emplace_back(reader.Next());
  }

  TableColumnDefinitions schema;
  schema.emplace_back("a_value", DataType::kInt, false);

  return std::make_shared<Table>(schema, std::move(chunks));
}

TEST_F(ExportOperatorTest, ExportToCsv) {
  const size_t num_chunks = 3;
  const ChunkOffset num_rows_per_chunk = 10;
  const std::string output_object_name = "output";
  auto storage = std::make_shared<MockStorage>();
  auto table = CreateTableContainingValue(num_chunks, num_rows_per_chunk, 1);
  auto mock_input_operator = std::make_shared<TableWrapper>(table);
  const std::string bucket_name = "MockBucket";

  auto export_operator =
      std::make_shared<ExportOperator>(mock_input_operator, bucket_name, output_object_name, ExportFormat::kCsv);

  auto operator_execution_context = std::make_shared<OperatorExecutionContext>(
      nullptr,
      [&storage, &bucket_name](const std::string& storage_name) {
        EXPECT_EQ(storage_name, bucket_name);
        return storage;
      },
      nullptr);

  EXPECT_NE(export_operator->Name(), "");

  mock_input_operator->Execute(operator_execution_context);
  export_operator->Execute(operator_execution_context);

  const ObjectStatus status = storage->GetStatus(output_object_name);
  EXPECT_FALSE(status.GetError().IsError());

  auto reader = storage->OpenForReading(output_object_name);
  size_t lines = 0;
  ByteBuffer buffer;
  reader->Read(0, ObjectReader::kLastByteInFile, &buffer);
  lines = std::count(buffer.CharData(), buffer.CharData() + buffer.Size(), '\n');

  reader->Close();

  EXPECT_EQ(lines, 1 /* Header line */ + num_chunks * num_rows_per_chunk);
}

TEST_F(ExportOperatorTest, OperatorWorksWithDifferentExportFormats) {
  const size_t num_chunks = 3;
  const ChunkOffset num_rows_per_chunk = 10;
  const std::string output_object_name = "output";
  auto table = CreateTableContainingValue(num_chunks, num_rows_per_chunk, 1);
  const std::array<ExportFormat, 3> formats = {ExportFormat::kCsv, ExportFormat::kOrc, ExportFormat::kOrcPartitioned};

  // Since every FormatWriter is tested separately we only need to check that we have valid code paths for each format
  // and some output is produced.
  for (const ExportFormat format : formats) {
    auto mock_input_operator = std::make_shared<TableWrapper>(table);
    auto storage = std::make_shared<MockStorage>();

    auto export_operator =
        std::make_shared<ExportOperator>(mock_input_operator, "MockBucket", output_object_name, format);

    auto operator_execution_context = std::make_shared<OperatorExecutionContext>(
        nullptr, [&storage](const std::string& /*storage_name*/) { return storage; }, nullptr);

    mock_input_operator->Execute(operator_execution_context);
    export_operator->Execute(operator_execution_context);

    const ObjectStatus status = storage->GetStatus(output_object_name);
    EXPECT_FALSE(status.GetError().IsError());
  }
}

}  // namespace skyrise
