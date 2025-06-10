#include "import_options.hpp"

#include <magic_enum/magic_enum.hpp>

#include "expression/expression_serialization.hpp"
#include "storage/formats/parquet_expression.hpp"
#include "utils/json.hpp"

namespace {

const std::string kJsonKeyExpectedSchema = "expected_schema";
const std::string kJsonKeyExpectedSchemaColumnName = "name";
const std::string kJsonKeyExpectedSchemaDataType = "data_type";
const std::string kJsonKeyExpectedSchemaNullable = "nullable";

// OrcFormatReaderOptions
const std::string kJsonKeyOrcFormatReaderOptions = "orc_format_reader_options";
const std::string kJsonKeyOrcIncludeColumns = "include_columns";
const std::string kJsonKeyOrcParseDatesAsString = "parse_dates_as_string";
const std::string kJsonKeyOrcSelectRowRange = "select_row_range";
const std::string kJsonKeyOrcSelectPartitionRange = "select_partition_range";

// CsvFormatReaderOptions
const std::string kJsonKeyCsvFormatReaderOptions = "csv_format_reader_options";
const std::string kJsonKeyCsvDelimiter = "delimiter";
const std::string kJsonKeyCsvGuessDelimiter = "guess_delimiter";
const std::string kJsonKeyCsvGuessHasHeader = "guess_has_header";
const std::string kJsonKeyCsvGuessHasTypes = "guess_has_types";
const std::string kJsonKeyCsvHasHeader = "has_header";
const std::string kJsonKeyCsvHasTypes = "has_types";
const std::string kJsonKeyCsvReadBufferSize = "read_buffer_size";

// ParquetFormatReaderOptions
const std::string kJsonKeyParquetFormatReaderOptions = "parquet_format_reader_options";
const std::string kJsonKeyParquetIncludeColumns = "include_columns";
const std::string kJsonKeyParquetParseDatesAsString = "parse_dates_as_string";
const std::string kJsonKeyParquetRowGroupIds = "row_group_ids";
const std::string kJsonKeyParquetExpression = "skyrise_expression";

}  // namespace

namespace skyrise {

ImportOptions::ImportOptions(ImportFormat object_format) : ImportOptions(object_format, std::vector<ColumnId>{}) {}

ImportOptions::ImportOptions(ImportFormat object_format, const std::vector<ColumnId>& columns)
    : ImportOptions(object_format, columns, std::vector<int32_t>{}) {}

ImportOptions::ImportOptions(ImportFormat object_format, const std::vector<ColumnId>& columns,
                             std::optional<std::vector<int32_t>> partitions)
    : import_format_(object_format) {
  // Use default reader options
  switch (import_format_) {
    case ImportFormat::kCsv: {
      reader_options_ = CsvFormatReaderOptions();
    } break;
    case ImportFormat::kOrc: {
      auto options = OrcFormatReaderOptions();
      options.include_columns = columns;
      reader_options_ = options;
    } break;
    case ImportFormat::kParquet: {
      auto options = ParquetFormatReaderOptions();
      options.include_columns = columns;
      if (partitions) {
        options.row_group_ids = *partitions;
      }
      reader_options_ = options;
    } break;
    default:
      Fail("Unexpected ImportFormat.");
  }
}

ImportOptions::ImportOptions(CsvFormatReaderOptions csv_format_reader_options)
    : import_format_(ImportFormat::kCsv), reader_options_(std::move(csv_format_reader_options)) {};

ImportOptions::ImportOptions(OrcFormatReaderOptions orc_format_reader_options)
    : import_format_(ImportFormat::kOrc), reader_options_(std::move(orc_format_reader_options)) {};

ImportOptions::ImportOptions(ParquetFormatReaderOptions parquet_format_reader_options)
    : import_format_(ImportFormat::kParquet), reader_options_(std::move(parquet_format_reader_options)) {};

Aws::Utils::Json::JsonValue ImportOptions::ToJson() const {
  Aws::Utils::Json::JsonValue json_output;

  switch (import_format_) {
    case ImportFormat::kCsv: {
      const auto& csv_options = std::get<CsvFormatReaderOptions>(reader_options_);
      auto json_csv_options = Aws::Utils::Json::JsonValue()
                                  .WithInt64(kJsonKeyCsvReadBufferSize, csv_options.read_buffer_size)
                                  .WithString(kJsonKeyCsvDelimiter, std::string{csv_options.delimiter})
                                  .WithBool(kJsonKeyCsvGuessDelimiter, csv_options.guess_delimiter)
                                  .WithBool(kJsonKeyCsvGuessHasHeader, csv_options.guess_has_header)
                                  .WithBool(kJsonKeyCsvGuessHasTypes, csv_options.guess_has_types)
                                  .WithBool(kJsonKeyCsvHasHeader, csv_options.has_header)
                                  .WithBool(kJsonKeyCsvHasTypes, csv_options.has_types);

      if (csv_options.expected_schema) {
        json_csv_options.WithArray(kJsonKeyExpectedSchema,
                                   TableColumnDefinitionsToJsonArray(csv_options.expected_schema));
      }

      json_output.WithObject(kJsonKeyCsvFormatReaderOptions, json_csv_options);
    } break;

    case ImportFormat::kOrc: {
      const auto& orc_options = std::get<OrcFormatReaderOptions>(reader_options_);
      auto json_orc_options =
          Aws::Utils::Json::JsonValue().WithBool(kJsonKeyOrcParseDatesAsString, orc_options.parse_dates_as_string);

      if (orc_options.expected_schema) {
        json_orc_options.WithArray(kJsonKeyExpectedSchema,
                                   TableColumnDefinitionsToJsonArray(orc_options.expected_schema));
      } else if (orc_options.include_columns.has_value()) {
        json_orc_options.WithArray(kJsonKeyOrcIncludeColumns,
                                   VectorToJsonArray<ColumnId>(orc_options.include_columns.value()));
      }

      json_output.WithObject(kJsonKeyOrcFormatReaderOptions, json_orc_options);

    } break;

    case ImportFormat::kParquet: {
      const auto& parquet_options = std::get<ParquetFormatReaderOptions>(reader_options_);
      auto json_parquet_options = Aws::Utils::Json::JsonValue();

      json_parquet_options.WithBool(kJsonKeyParquetParseDatesAsString, parquet_options.parse_dates_as_string);

      if (parquet_options.include_columns.has_value()) {
        json_parquet_options.WithArray(kJsonKeyParquetIncludeColumns,
                                       VectorToJsonArray<ColumnId>(parquet_options.include_columns.value()));
      }

      if (parquet_options.expected_schema) {
        json_parquet_options.WithArray(kJsonKeyExpectedSchema,
                                       TableColumnDefinitionsToJsonArray(parquet_options.expected_schema));
      }

      if (parquet_options.row_group_ids.has_value()) {
        json_parquet_options.WithArray(kJsonKeyParquetRowGroupIds,
                                       VectorToJsonArray<int32_t>(parquet_options.row_group_ids.value()));
      }

      if (parquet_options.skyrise_expression.has_value()) {
        json_parquet_options.WithObject(kJsonKeyParquetExpression,
                                        SerializeExpression(parquet_options.skyrise_expression.value()));
      }

      json_output.WithObject(kJsonKeyParquetFormatReaderOptions, json_parquet_options);
    } break;

    default:
      Fail("Unexpected ImportFormat.");
  }

  return json_output;
}

std::shared_ptr<const ImportOptions> ImportOptions::FromJson(const Aws::Utils::Json::JsonView& json_in) {
  // (a) CSV Options
  if (json_in.ValueExists(kJsonKeyCsvFormatReaderOptions)) {
    const auto json = json_in.GetObject(kJsonKeyCsvFormatReaderOptions);
    CsvFormatReaderOptions csv_options;

    csv_options.read_buffer_size = json.GetInt64(kJsonKeyCsvReadBufferSize);
    csv_options.delimiter = json.GetString(kJsonKeyCsvDelimiter)[0];
    // GetBool() always returns false here. Therefore, boolean variables must be retrieved as objects and then
    // cast to bool.
    csv_options.guess_delimiter = json.GetObject(kJsonKeyCsvGuessDelimiter).AsBool();
    csv_options.guess_has_header = json.GetObject(kJsonKeyCsvGuessHasHeader).AsBool();
    csv_options.guess_has_types = json.GetObject(kJsonKeyCsvGuessHasTypes).AsBool();
    csv_options.has_header = json.GetObject(kJsonKeyCsvHasHeader).AsBool();
    csv_options.has_types = json.GetObject(kJsonKeyCsvHasTypes).AsBool();

    if (json.KeyExists(kJsonKeyExpectedSchema)) {
      csv_options.expected_schema =
          ImportOptions::TableColumnDefinitionsFromJsonArray(json.GetArray(kJsonKeyExpectedSchema));
    }

    return std::make_shared<ImportOptions>(csv_options);
  }

  // (b) ORC Options
  if (json_in.ValueExists(kJsonKeyOrcFormatReaderOptions)) {
    const auto json = json_in.GetObject(kJsonKeyOrcFormatReaderOptions);
    OrcFormatReaderOptions orc_options;
    orc_options.parse_dates_as_string = json.GetObject(kJsonKeyOrcParseDatesAsString).AsBool();

    if (json.KeyExists(kJsonKeyExpectedSchema)) {
      orc_options.expected_schema =
          ImportOptions::TableColumnDefinitionsFromJsonArray(json.GetArray(kJsonKeyExpectedSchema));
    } else if (json.KeyExists(kJsonKeyOrcIncludeColumns)) {
      orc_options.include_columns = JsonArrayToVector<ColumnId>(json.GetArray(kJsonKeyOrcIncludeColumns));
    }

    return std::make_shared<ImportOptions>(orc_options);
  }

  // (c) PARQUET Options
  if (json_in.ValueExists(kJsonKeyParquetFormatReaderOptions)) {
    const auto json = json_in.GetObject(kJsonKeyParquetFormatReaderOptions);
    ParquetFormatReaderOptions parquet_options;
    parquet_options.parse_dates_as_string = json.GetObject(kJsonKeyParquetParseDatesAsString).AsBool();

    if (json.KeyExists(kJsonKeyExpectedSchema)) {
      parquet_options.expected_schema =
          ImportOptions::TableColumnDefinitionsFromJsonArray(json.GetArray(kJsonKeyExpectedSchema));
    }

    if (json.KeyExists(kJsonKeyParquetIncludeColumns)) {
      parquet_options.include_columns = JsonArrayToVector<ColumnId>(json.GetArray(kJsonKeyParquetIncludeColumns));
    }

    if (json.KeyExists(kJsonKeyParquetExpression)) {
      const auto serialized_expression = json.GetObject(kJsonKeyParquetExpression);
      const auto skyrise_expression = DeserializeExpression(serialized_expression);
      parquet_options.arrow_expression = CreateArrowExpression(skyrise_expression);
    }

    if (json.KeyExists(kJsonKeyParquetRowGroupIds)) {
      parquet_options.row_group_ids = JsonArrayToVector<int32_t>(json.GetArray(kJsonKeyParquetRowGroupIds));
    }

    return std::make_shared<ImportOptions>(parquet_options);
  }

  Fail("Failed to create ImportOptions because JSON values are missing.");
}

std::shared_ptr<AbstractChunkReaderFactory> ImportOptions::CreateReaderFactory() const {
  switch (import_format_) {
    case ImportFormat::kCsv:
      return std::make_shared<FormatReaderFactory<CsvFormatReader>>(std::get<CsvFormatReaderOptions>(reader_options_));
    case ImportFormat::kOrc:
      return std::make_shared<FormatReaderFactory<OrcFormatReader>>(std::get<OrcFormatReaderOptions>(reader_options_));
    case ImportFormat::kParquet:
      return std::make_shared<FormatReaderFactory<ParquetFormatReader>>(
          std::get<ParquetFormatReaderOptions>(reader_options_));
    default:
      Fail("Unexpected ImportFormat.");
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> ImportOptions::TableColumnDefinitionsToJsonArray(
    const std::shared_ptr<TableColumnDefinitions>& column_definitions) {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> json_output(column_definitions->size());

  for (size_t i = 0; i < column_definitions->size(); ++i) {
    const auto& column_definition = (*column_definitions)[i];
    json_output[i] =
        Aws::Utils::Json::JsonValue()
            .WithString(kJsonKeyExpectedSchemaColumnName, column_definition.name)
            .WithString(kJsonKeyExpectedSchemaDataType, std::string(magic_enum::enum_name(column_definition.data_type)))
            .WithBool(kJsonKeyExpectedSchemaNullable, column_definition.nullable);
  }

  return json_output;
}

std::shared_ptr<TableColumnDefinitions> ImportOptions::TableColumnDefinitionsFromJsonArray(
    const Aws::Utils::Array<Aws::Utils::Json::JsonView>& json_array) {
  Assert(json_array.GetLength(), "Expected JSON array with at least one entry.");
  auto column_definitions = std::make_shared<TableColumnDefinitions>();
  column_definitions->reserve(json_array.GetLength());

  for (size_t i = 0; i < json_array.GetLength(); ++i) {
    Assert(json_array[i].ValueExists(kJsonKeyExpectedSchemaColumnName),
           "Expected JSON value " + kJsonKeyExpectedSchemaColumnName);
    const auto column_name = json_array[i].GetString(kJsonKeyExpectedSchemaColumnName);
    Assert(json_array[i].ValueExists(kJsonKeyExpectedSchemaDataType),
           "Expected JSON value " + kJsonKeyExpectedSchemaDataType);
    const auto data_type =
        magic_enum::enum_cast<DataType>(json_array[i].GetString(kJsonKeyExpectedSchemaDataType)).value();
    Assert(json_array[i].ValueExists(kJsonKeyExpectedSchemaNullable),
           "Expected JSON value " + kJsonKeyExpectedSchemaNullable);
    const auto nullable = json_array[i].GetBool(kJsonKeyExpectedSchemaNullable);

    column_definitions->emplace_back(column_name, data_type, nullable);
  }

  return column_definitions;
}

}  // namespace skyrise
