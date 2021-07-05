#include "storage/table/chunk_reader.hpp"

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/backend/testdata_storage.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/mock_chunk_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class ChunkReaderTest : public ::testing::Test {
  using ValueSegmentGenerators = std::vector<ValueSegmentGenerator>;

  void SetUp() override {
    ValueSegmentGenerators segment_generators;

    const auto string_segment_generator_callback = []() {
      return std::make_shared<ValueSegment<std::string>>(std::vector<std::string>(kSizeSegments, "Test"));
    };

    const auto int_segment_generator_callback = []() {
      return std::make_shared<ValueSegment<int32_t>>(std::vector<int32_t>(kSizeSegments, 5));
    };

    const auto float_segment_generator_callback = []() {
      return std::make_shared<ValueSegment<float>>(std::vector<float>(kSizeSegments, 5.0f));
    };

    segment_generators.emplace_back(string_segment_generator_callback);
    segment_generators.emplace_back(int_segment_generator_callback);
    segment_generators.emplace_back(float_segment_generator_callback);

    auto schema = std::make_shared<TableColumnDefinitions>();
    schema->emplace_back("mock_schema", DataType::kString, false);
    schema->emplace_back("mock_schema_two", DataType::kFloat, false);

    mock_formatter_configuration_.num_chunks = kNumChunksMockFormatter;
    mock_formatter_configuration_.generators = segment_generators;
    mock_formatter_configuration_.schema = schema;

    mock_formatter_configuration_error_.num_chunks = kNumChunksMockFormatter;
    mock_formatter_configuration_error_.generators = segment_generators;
    mock_formatter_configuration_error_.error = true;
    mock_formatter_configuration_error_.num_chunks_until_error = kNumChunksUntilErrorMockFormatter;

    mock_formatter_configuration_initialization_error_.num_chunks = kNumChunksMockFormatter;
    mock_formatter_configuration_initialization_error_.generators = segment_generators;
    mock_formatter_configuration_initialization_error_.error = true;
    mock_formatter_configuration_initialization_error_.num_chunks_until_error = 0;

    orc_options_.parse_dates_as_string = true;

    csv_options_.has_header = true;
    csv_options_.has_types = true;

    mock_storage_ = std::make_shared<MockStorage>();
  }

 protected:
  static constexpr size_t kNumSequentialTasks = 3;
  static constexpr size_t kNumChunksMockFormatter = 3;
  static constexpr size_t kNumChunksUntilErrorMockFormatter = 2;
  static constexpr size_t kSizeSegments = 100;

  CsvFormatReaderOptions csv_options_;
  OrcFormatReaderOptions orc_options_;

  MockChunkReaderConfiguration mock_formatter_configuration_;
  MockChunkReaderConfiguration mock_formatter_configuration_error_;
  MockChunkReaderConfiguration mock_formatter_configuration_initialization_error_;
  std::shared_ptr<MockStorage> mock_storage_;
};

TEST_F(ChunkReaderTest, GetFormatReaderFactoryWithDefaultConfiguration) {
  // This tests, if the following code compiles.
  [[maybe_unused]] auto factory = std::make_shared<FormatReaderFactory<MockChunkReader>>();
}

TEST_F(ChunkReaderTest, ChunkErrorTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_error_);
  reader.AddObjects(format_factory, mock_storage_, {"file1"});

  size_t counter_chunk = 0;
  for (; reader.HasNext(); ++counter_chunk) {
    auto chunk = reader.Next();
  }
  EXPECT_TRUE(reader.HasError());
  EXPECT_EQ(counter_chunk, kNumChunksUntilErrorMockFormatter);
}

TEST_F(ChunkReaderTest, InitializationErrorTest) {
  ChunkReader reader;

  auto format_factory =
      std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_initialization_error_);
  reader.AddObjects(format_factory, mock_storage_, {"file1"});

  size_t counter_chunk = 0;
  for (; reader.HasNext(); ++counter_chunk) {
    auto chunk = reader.Next();
  }
  EXPECT_TRUE(reader.HasError());
  EXPECT_EQ(counter_chunk, 0);
}

TEST_F(ChunkReaderTest, FormatterTypeTest) {
  ChunkReader reader;

  auto test_data = std::make_shared<TestdataStorage>();
  auto orc_factory = std::make_shared<FormatReaderFactory<OrcFormatReader>>(orc_options_);
  reader.AddObjects(orc_factory, test_data, {"orc/with_types.orc"});

  auto csv_factory = std::make_shared<FormatReaderFactory<CsvFormatReader>>(csv_options_);
  reader.AddObjects(csv_factory, test_data, {"csv/with_types.csv"});

  EXPECT_FALSE(reader.HasError());
  EXPECT_TRUE(reader.HasNext());

  size_t chunk_counter = 0;
  while (reader.HasNext()) {
    auto chunk = reader.Next();
    if (chunk != nullptr) {
      chunk_counter++;
    }
  }

  ASSERT_FALSE(reader.HasError());
  ASSERT_EQ(chunk_counter, 2);
}

TEST_F(ChunkReaderTest, EmptyObjectListTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
  reader.AddObjects(format_factory, mock_storage_, {});

  ASSERT_FALSE(reader.HasError());
  ASSERT_FALSE(reader.HasNext());
  ASSERT_EQ(reader.Next(), nullptr);
}

TEST_F(ChunkReaderTest, DiscoverSchemaTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
  reader.AddObjects(format_factory, mock_storage_, {"test1"});

  EXPECT_FALSE(reader.HasError());
  EXPECT_TRUE(reader.HasNext());

  auto schema = reader.GetSchema();
  ASSERT_EQ(schema->size(), 2);

  while (reader.HasNext()) {
    reader.Next();
  }

  EXPECT_FALSE(reader.HasError());
}

TEST_F(ChunkReaderTest, DiscoverSchemaDataTypeErrorTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
  reader.AddObjects(format_factory, mock_storage_, {"test1"});

  auto schema = std::make_shared<TableColumnDefinitions>();
  schema->emplace_back("mock_schema", DataType::kString, false);
  schema->emplace_back("mock_schema_two", DataType::kLong, false);

  MockChunkReaderConfiguration mock_configuration = mock_formatter_configuration_;
  mock_configuration.schema = schema;

  auto format_factory_two = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_configuration);
  reader.AddObjects(format_factory_two, mock_storage_, {"test2"});

  EXPECT_FALSE(reader.HasError());
  EXPECT_TRUE(reader.HasNext());

  while (reader.HasNext()) {
    reader.Next();
  }

  EXPECT_TRUE(reader.HasError());
}

TEST_F(ChunkReaderTest, DiscoverSchemaNullableErrorTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
  reader.AddObjects(format_factory, mock_storage_, {"test1"});

  auto schema = std::make_shared<TableColumnDefinitions>();
  schema->emplace_back("mock_schema", DataType::kString, false);
  schema->emplace_back("mock_schema_two", DataType::kString, true);

  MockChunkReaderConfiguration mock_configuration = mock_formatter_configuration_;
  mock_configuration.schema = schema;

  auto format_factory_two = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_configuration);
  reader.AddObjects(format_factory_two, mock_storage_, {"test2"});

  EXPECT_FALSE(reader.HasError());
  EXPECT_TRUE(reader.HasNext());

  while (reader.HasNext()) {
    reader.Next();
  }

  EXPECT_TRUE(reader.HasError());
}

TEST_F(ChunkReaderTest, DiscoverSchemaSizeErrorTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
  reader.AddObjects(format_factory, mock_storage_, {"test1"});

  auto schema = std::make_shared<TableColumnDefinitions>();
  schema->emplace_back("mock_schema", DataType::kString, false);

  MockChunkReaderConfiguration mock_configuration = mock_formatter_configuration_;
  mock_configuration.schema = schema;

  auto format_factory_two = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_configuration);
  reader.AddObjects(format_factory_two, mock_storage_, {"test2"});

  EXPECT_FALSE(reader.HasError());
  EXPECT_TRUE(reader.HasNext());

  while (reader.HasNext()) {
    reader.Next();
  }

  EXPECT_TRUE(reader.HasError());
}

TEST_F(ChunkReaderTest, DiscoverSchemaNameErrorTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
  reader.AddObjects(format_factory, mock_storage_, {"test1"});

  auto schema = std::make_shared<TableColumnDefinitions>();
  schema->emplace_back("mock_schema", DataType::kString, false);
  schema->emplace_back("mock_schema_two_wrong", DataType::kFloat, false);

  MockChunkReaderConfiguration mock_configuration = mock_formatter_configuration_;
  mock_configuration.schema = schema;

  auto format_factory_two = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_configuration);
  reader.AddObjects(format_factory_two, mock_storage_, {"test2"});

  EXPECT_FALSE(reader.HasError());
  EXPECT_TRUE(reader.HasNext());

  while (reader.HasNext()) {
    reader.Next();
  }

  EXPECT_TRUE(reader.HasError());
}

TEST_F(ChunkReaderTest, EmptyReaderTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
  reader.AddObjects(format_factory, mock_storage_, {"test1"});

  auto schema = std::make_shared<TableColumnDefinitions>();
  schema->emplace_back("mock_schema", DataType::kString, false);

  MockChunkReaderConfiguration mock_configuration = mock_formatter_configuration_;
  mock_configuration.num_chunks = 0;

  auto format_factory_two = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_configuration);
  reader.AddObjects(format_factory_two, mock_storage_, {"test2"});

  EXPECT_FALSE(reader.HasError());
  EXPECT_TRUE(reader.HasNext());

  size_t chunk_counter = 0;
  while (reader.HasNext()) {
    auto chunk = reader.Next();
    if (chunk != nullptr) {
      chunk_counter++;
    }
  }

  EXPECT_EQ(chunk_counter, kNumChunksMockFormatter);
  EXPECT_FALSE(reader.HasError());
}

TEST_F(ChunkReaderTest, DefaultConfigurationTest) {
  ChunkReader reader;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>();
  reader.AddObjects(format_factory, mock_storage_, {"test1"});

  EXPECT_FALSE(reader.HasError());
  EXPECT_TRUE(reader.HasNext());

  size_t chunk_counter = 0;
  while (reader.HasNext()) {
    auto chunk = reader.Next();
    if (chunk != nullptr) {
      chunk_counter++;
    }
  }

  EXPECT_EQ(chunk_counter, kNumChunksMockFormatter);
  EXPECT_FALSE(reader.HasError());
}

}  // namespace skyrise
