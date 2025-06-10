#include "operator/export_operator.hpp"

#include <string_view>

#include <gtest/gtest.h>

#include "operator/table_wrapper.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/backend/mock_storage.hpp"
#include "storage/formats/mock_chunk_reader.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/value_segment.hpp"

namespace {

const std::string kOrcIdentifier = "ORC";
const std::string kParquetIdentifier = "PAR1";

std::shared_ptr<const skyrise::Table> CreateTableContainingValue(size_t num_chunks,
                                                                 skyrise::ChunkOffset num_rows_per_chunk,
                                                                 int32_t value) {
  skyrise::MockChunkReaderConfiguration config;
  config.num_chunks = num_chunks;
  config.generators.emplace_back([num_rows_per_chunk, value]() {
    return std::make_shared<skyrise::ValueSegment<int32_t>>(std::vector<int32_t>(num_rows_per_chunk, value));
  });
  skyrise::MockChunkReader reader(nullptr, 0, config);
  std::vector<std::shared_ptr<skyrise::Chunk>> chunks;
  while (reader.HasNext()) {
    chunks.emplace_back(reader.Next());
  }

  skyrise::TableColumnDefinitions schema;
  schema.emplace_back("a_value", skyrise::DataType::kInt, false);

  return std::make_shared<skyrise::Table>(schema, std::move(chunks));
}

bool VerifyWrittenData(const skyrise::FileFormat export_format, skyrise::ByteBuffer* written_data,
                       const size_t num_chunks, const skyrise::ChunkOffset num_rows_per_chunk) {
  switch (export_format) {
    case skyrise::FileFormat::kCsv: {
      // Verify the correct number of lines in the CVS file.
      size_t lines = std::count(written_data->CharData(), written_data->CharData() + written_data->Size(), '\n');
      return lines == 1 /* Header line */ + num_chunks * num_rows_per_chunk;
    }
    case skyrise::FileFormat::kOrc: {
      // Verify that file begins with ORC's identifier.
      return std::string(written_data->CharData()).substr(0, 3) == kOrcIdentifier;
    }
    case skyrise::FileFormat::kParquet: {
      // Verify that file begins with Parquet's identifier.
      return std::string(written_data->CharData()).substr(0, 4) == kParquetIdentifier;
    }
    default:
      Fail("Format not covered.");
  }
}

}  // namespace

namespace skyrise {

class ExportOperatorTest : public ::testing::TestWithParam<FileFormat> {};

INSTANTIATE_TEST_SUITE_P(ExportOperatorTest, ExportOperatorTest,
                         ::testing::Values(FileFormat::kCsv, FileFormat::kOrc, FileFormat::kParquet),
                         [](const ::testing::TestParamInfo<ExportOperatorTest::ParamType>& info) {
                           // Set the identifier of the test. Helps for debugging.
                           if (info.param == FileFormat::kOrc) {
                             return "Orc";
                           }
                           return info.param == FileFormat::kParquet ? "Parquet" : "Csv";
                         });

TEST_P(ExportOperatorTest, ExportToFormat) {
  const FileFormat export_format = GetParam();
  const size_t num_chunks = 3;
  const ChunkOffset num_rows_per_chunk = 10;
  const std::string output_object_name = "output";
  auto storage = std::make_shared<MockStorage>();
  auto table = CreateTableContainingValue(num_chunks, num_rows_per_chunk, 1);
  auto mock_input_operator = std::make_shared<TableWrapper>(table);
  const std::string bucket_name = "MockBucket";

  auto export_operator =
      std::make_shared<ExportOperator>(mock_input_operator, bucket_name, output_object_name, export_format);

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
  ByteBuffer buffer;
  reader->Read(0, ObjectReader::kLastByteInFile, &buffer);

  reader->Close();

  EXPECT_TRUE(VerifyWrittenData(export_format, &buffer, num_chunks, num_rows_per_chunk));
}

TEST_F(ExportOperatorTest, OperatorWorksWithDifferentExportFormats) {
  const size_t num_chunks = 3;
  const ChunkOffset num_rows_per_chunk = 10;
  const std::string output_object_name = "output";
  auto table = CreateTableContainingValue(num_chunks, num_rows_per_chunk, 1);
  const std::array<FileFormat, 3> formats = {FileFormat::kCsv, FileFormat::kOrc, FileFormat::kParquet};

  // Since every FormatWriter is tested separately we only need to check that we have valid code paths for each format
  // and some output is produced.
  for (const FileFormat format : formats) {
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
