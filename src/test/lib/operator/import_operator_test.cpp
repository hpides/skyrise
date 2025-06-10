#include "operator/import_operator.hpp"

#include <string_view>

#include <gtest/gtest.h>

#include "scheduler/worker/fragment_scheduler.hpp"
#include "storage/backend/mock_storage.hpp"
#include "storage/backend/test_storage.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/mock_chunk_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/formats/parquet_writer.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"

namespace {

const size_t kNumChunksMockFormatter = 3;
const size_t kSizeSegments = 100;
const std::string kCsvTypesPath = "csv/with_types.csv";
const std::string kOrcTypesPath = "orc/with_types.orc";
const std::string kParquetTypesPath = "parquet/with_types.parquet";

}  // namespace

namespace skyrise {

class ImportOperatorTest : public ::testing::Test {
 public:
  void SetupMockStorage(const size_t number_chunks = kNumChunksMockFormatter,
                        const size_t segment_size = kSizeSegments) {
    mock_schema_ = std::make_shared<TableColumnDefinitions>();
    mock_schema_->emplace_back("mock_schema", DataType::kString, false);
    mock_schema_->emplace_back("mock_schema_two", DataType::kString, false);
    mock_schema_->emplace_back("mock_schema_three", DataType::kString, false);

    auto string_segment = std::make_shared<ValueSegment<std::string>>(std::vector<std::string>(segment_size, "Test"));
    auto chunk = std::make_shared<Chunk>(Segments({string_segment, string_segment, string_segment}));

    std::stringstream output;
    std::shared_ptr<std::stringstream> output_ptr(&output, [](auto /*unused*/) {});
    ParquetFormatWriter formatter;
    formatter.SetOutputHandler([&output_ptr](const char* data, size_t size) { output_ptr->write(data, size); });
    formatter.Initialize(*mock_schema_);
    for (size_t i = 0; i < number_chunks; ++i) {
      formatter.ProcessChunk(chunk);
    }
    formatter.Finalize();
    output.seekg(0, std::ios::end);
    int size = output.tellg();
    output.seekg(0, std::ios::beg);

    mock_storage_ = std::make_shared<MockStorage>();
    auto writer_a = mock_storage_->OpenForWriting("a");
    auto writer_b = mock_storage_->OpenForWriting("b");
    auto writer_c = mock_storage_->OpenForWriting("c");
    writer_a->Write(output.str().c_str(), size);
    writer_b->Write(output.str().c_str(), size);
    writer_c->Write(output.str().c_str(), size);
    writer_a->Close();
    writer_b->Close();
    writer_c->Close();
  }

  void SetUp() override {
    SetupMockStorage();
    orc_options_.parse_dates_as_string = true;

    csv_options_.has_header = true;
    csv_options_.has_types = true;

    test_storage_ = std::make_shared<TestStorage>();

    test_schema_ = std::make_shared<TableColumnDefinitions>();
    test_schema_->emplace_back("a_int", DataType::kInt, false);
    test_schema_->emplace_back("a_long", DataType::kLong, false);
    test_schema_->emplace_back("a_float", DataType::kFloat, false);
    test_schema_->emplace_back("a_double", DataType::kDouble, false);
    test_schema_->emplace_back("a_string", DataType::kString, false);
    scheduler_ = std::make_shared<FragmentScheduler>();
    operator_execution_context_ = std::make_shared<OperatorExecutionContext>(
        nullptr,
        [this](const std::string& storage_name) -> std::shared_ptr<Storage> {
          if (storage_name == "MockStorage") {
            return mock_storage_;
          } else if (storage_name == "TestStorage") {
            return test_storage_;
          }
          Fail("Storage type is unknown in this context: " + storage_name);
        },
        [this]() { return scheduler_; });
  }

  static void TestImportOperator(ImportOperator* import_operator, const TableColumnDefinitions& schema,
                                 size_t num_chunks, size_t row_count, std::vector<ColumnId>* included_column_ids,
                                 const std::shared_ptr<OperatorExecutionContext>& operator_execution_context,
                                 bool check_schema_nullability = true) {
    import_operator->Execute(operator_execution_context);
    auto table = import_operator->GetOutput();

    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->ChunkCount(), num_chunks);
    EXPECT_EQ(table->RowCount(), row_count);

    const std::optional<std::string> name = import_operator->Name();
    EXPECT_TRUE(name.has_value());
    // NOLINTNEXTLINE(modernize-use-ranges)
    std::sort(included_column_ids->begin(), included_column_ids->end());

    std::vector<ColumnId> pruned_column_ids;
    ExtractColumns(schema, *included_column_ids, &pruned_column_ids);
    for (const auto& column_id : pruned_column_ids) {
      EXPECT_ANY_THROW(table->ColumnIdByName(schema[column_id].name));
    }

    for (size_t counter = 0; counter < included_column_ids->size(); ++counter) {
      EXPECT_EQ(schema[(*included_column_ids)[counter]].data_type, table->ColumnDataType(counter));
      if (check_schema_nullability) {
        EXPECT_EQ(schema[(*included_column_ids)[counter]].nullable, table->ColumnIsNullable(counter));
      }
    }

    for (ChunkId chunk_counter = 0; chunk_counter < num_chunks; ++chunk_counter) {
      const auto chunk = table->GetChunk(chunk_counter);
      EXPECT_EQ(chunk->GetColumnCount(), included_column_ids->size());
    }
  }

  static void ExtractColumns(const TableColumnDefinitions& schema, const std::vector<ColumnId>& included_column_ids,
                             std::vector<ColumnId>* pruned_column_ids) {
    auto iter = included_column_ids.begin();

    for (ColumnId column_id = 0; column_id < schema.size(); ++column_id) {
      if (iter != included_column_ids.end() && column_id == *iter) {
        ++iter;
      } else {
        pruned_column_ids->push_back(column_id);
      }
    }
  }

  //  NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
  void SetupMockedImportOperator(std::vector<std::string>&& object_keys, std::vector<ColumnId>&& included_column_ids) {
    auto format_factory = std::make_shared<FormatReaderFactory<ParquetFormatReader>>();
    std::vector<ObjectReference> object_references;
    object_references.reserve(object_keys.size());
    for (const std::string& object_key : object_keys) {
      object_references.emplace_back("MockStorage", object_key, "");
    }

    ImportOperator import_operator(object_references, included_column_ids, format_factory, ImportFormat::kParquet);

    const size_t num_chunks = kNumChunksMockFormatter * object_keys.size();
    const size_t row_count = kSizeSegments * num_chunks;
    TestImportOperator(&import_operator, *mock_schema_, num_chunks, row_count, &included_column_ids,
                       operator_execution_context_);
  }

 protected:
  CsvFormatReaderOptions csv_options_;
  OrcFormatReaderOptions orc_options_;
  ParquetFormatReaderOptions parquet_options_;

  std::shared_ptr<TableColumnDefinitions> test_schema_;
  std::shared_ptr<TestStorage> test_storage_;

  std::shared_ptr<OperatorExecutionContext> operator_execution_context_;

  MockChunkReaderConfiguration mock_formatter_configuration_;
  std::shared_ptr<MockStorage> mock_storage_;
  std::shared_ptr<TableColumnDefinitions> mock_schema_;
  std::shared_ptr<FragmentScheduler> scheduler_;
};

TEST_F(ImportOperatorTest, PrunedImport) {
  SetupMockedImportOperator({"a", "b"}, {ColumnId(0), ColumnId(1), ColumnId(2)});
  SetupMockedImportOperator({"a", "b"}, {ColumnId(0), ColumnId(2)});
  SetupMockedImportOperator({"a"}, {ColumnId(1)});
}

TEST_F(ImportOperatorTest, LargeIncludedColumnIdsList) {
  auto format_factory = std::make_shared<FormatReaderFactory<ParquetFormatReader>>();
  const std::vector<ObjectReference> object_references = {ObjectReference{"MockStorage", "a", ""}};
  const std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(1), ColumnId(2), ColumnId(3)};
  ImportOperator import_operator(object_references, included_column_ids, format_factory, ImportFormat::kParquet);

  EXPECT_ANY_THROW(import_operator.Execute(operator_execution_context_));
}

TEST_F(ImportOperatorTest, EmptyIncludedColumnIdsList) {
  auto format_factory = std::make_shared<FormatReaderFactory<ParquetFormatReader>>();
  const std::vector<ObjectReference> object_references = {ObjectReference{"MockStorage", "a", ""}};
  ImportOperator import_operator(object_references, {}, format_factory, ImportFormat::kParquet);

  EXPECT_ANY_THROW(import_operator.Execute(operator_execution_context_));
}

TEST_F(ImportOperatorTest, ImportEmptyChunks) {
  const size_t number_chunks = 1;
  SetupMockStorage(number_chunks, 0);
  auto format_factory = std::make_shared<FormatReaderFactory<ParquetFormatReader>>();
  const std::vector<ObjectReference> object_references = {ObjectReference{"MockStorage", "a", ""},
                                                          ObjectReference{"MockStorage", "b", ""}};
  std::vector<ColumnId> included_column_ids = {ColumnId(0)};
  ImportOperator import_operator(object_references, included_column_ids, format_factory, ImportFormat::kParquet);

  const size_t num_chunks = number_chunks * object_references.size();
  const size_t row_count = 0;
  TestImportOperator(&import_operator, *mock_schema_, num_chunks, row_count, &included_column_ids,
                     operator_execution_context_);
}

TEST_F(ImportOperatorTest, ImportCsv) {
  auto csv_factory = std::make_shared<FormatReaderFactory<CsvFormatReader>>(csv_options_);

  const std::vector<ObjectReference> object_references = {ObjectReference{"TestStorage", kCsvTypesPath, ""}};
  std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(1), ColumnId(3), ColumnId(4)};
  ImportOperator import_operator(object_references, included_column_ids, csv_factory, ImportFormat::kCsv);

  TestImportOperator(&import_operator, *test_schema_, 1, 1, &included_column_ids, operator_execution_context_);
}

TEST_F(ImportOperatorTest, ImportOrc) {
  auto orc_factory = std::make_shared<FormatReaderFactory<OrcFormatReader>>(orc_options_);

  const std::vector<ObjectReference> object_references = {ObjectReference{"TestStorage", kOrcTypesPath, ""}};
  std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)};
  ImportOperator import_operator(object_references, included_column_ids, orc_factory, ImportFormat::kOrc);

  // Arrow ORC does not support writing schema nullability.
  const bool check_schema_nullability = false;
  TestImportOperator(&import_operator, *test_schema_, 1, 1, &included_column_ids, operator_execution_context_,
                     check_schema_nullability);
}

TEST_F(ImportOperatorTest, ImportParquet) {
  auto parquet_factory = std::make_shared<FormatReaderFactory<ParquetFormatReader>>(parquet_options_);

  const std::vector<ObjectReference> object_references = {ObjectReference{"TestStorage", kParquetTypesPath, ""}};
  std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)};
  ImportOperator import_operator(object_references, included_column_ids, parquet_factory, ImportFormat::kParquet);

  // Arrow Parquet assumes schema nullability if it is not explicitly deactivated.
  const bool check_schema_nullability = false;
  TestImportOperator(&import_operator, *test_schema_, 1, 1, &included_column_ids, operator_execution_context_,
                     check_schema_nullability);
}

TEST_F(ImportOperatorTest, OrcProjectionPushdown) {
  std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(2), ColumnId(3)};
  orc_options_.include_columns = std::vector<ColumnId>{0, 2, 3};
  auto orc_factory = std::make_shared<FormatReaderFactory<OrcFormatReader>>(orc_options_);

  const std::vector<ObjectReference> object_references = {ObjectReference{"TestStorage", kOrcTypesPath, ""}};
  ImportOperator import_operator(object_references, included_column_ids, orc_factory, ImportFormat::kOrc);

  // Arrow ORC does not support writing schema nullability.
  const bool check_schema_nullability = false;
  TestImportOperator(&import_operator, *test_schema_, 1, 1, &included_column_ids, operator_execution_context_,
                     check_schema_nullability);
}

}  // namespace skyrise
