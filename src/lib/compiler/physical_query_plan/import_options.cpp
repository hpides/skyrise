#include "import_options.hpp"

#include <magic_enum.hpp>

namespace {

const std::string kJsonKeyExpectedSchema = "expected_schema";
const std::string kJsonKeyExpectedSchemaColumnName = "name";
const std::string kJsonKeyExpectedSchemaDataType = "data_type";
const std::string kJsonKeyExpectedSchemaNullable = "nullable";

// OrcFormatReaderOptions
const std::string kJsonKeyOrcFormatReaderOptions = "orc_format_reader_options";
const std::string kJsonKeyOrcParseDatesAsString = "parse_dates_as_string";
const std::string kJsonKeyOrcSelectRowRange = "select_row_range";
const std::string kJsonKeyOrcRangeBegin = "range_begin";
const std::string kJsonKeyOrcRangeEnd = "range_end";
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

}  // namespace

namespace skyrise {

ImportOptions::ImportOptions(ImportFormat import_format) : import_format_(import_format) {
  // Use default reader options
  switch (import_format_) {
    case ImportFormat::kCsv: {
      reader_options_ = CsvFormatReaderOptions();
    } break;
    case ImportFormat::kOrc: {
      reader_options_ = OrcFormatReaderOptions();
    } break;
    default:
      Fail("Unexpected ImportFormat.");
  }
}

ImportOptions::ImportOptions(CsvFormatReaderOptions csv_format_reader_options)
    : import_format_(ImportFormat::kCsv), reader_options_(std::move(csv_format_reader_options)){};

ImportOptions::ImportOptions(OrcFormatReaderOptions orc_format_reader_options)
    : import_format_(ImportFormat::kOrc), reader_options_(std::move(orc_format_reader_options)){};

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
      }

      if (orc_options.select_row_range.has_value()) {
        const auto [row_range_begin, row_range_end] = orc_options.select_row_range.value();
        json_orc_options.WithObject(kJsonKeyOrcSelectRowRange, Aws::Utils::Json::JsonValue()
                                                                   .WithInt64(kJsonKeyOrcRangeBegin, row_range_begin)
                                                                   .WithInt64(kJsonKeyOrcRangeEnd, row_range_end));
      } else if (orc_options.select_partition_range.has_value()) {
        const auto [partition_range_begin, partition_range_end] = orc_options.select_partition_range.value();
        json_orc_options.WithObject(kJsonKeyOrcSelectPartitionRange,
                                    Aws::Utils::Json::JsonValue()
                                        .WithInt64(kJsonKeyOrcRangeBegin, partition_range_begin)
                                        .WithInt64(kJsonKeyOrcRangeEnd, partition_range_end));
      }

      json_output.WithObject(kJsonKeyOrcFormatReaderOptions, json_orc_options);

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
    // casted to bool.
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
    }

    const auto get_range = [&](const std::string& key) -> std::optional<std::pair<size_t, size_t>> {
      if (json.ValueExists(key)) {
        const auto row_range = json.GetObject(key);
        return std::make_pair(row_range.GetInt64(kJsonKeyOrcRangeBegin), row_range.GetInt64(kJsonKeyOrcRangeEnd));
      }

      return std::nullopt;
    };
    orc_options.select_row_range = get_range(kJsonKeyOrcSelectRowRange);
    orc_options.select_partition_range = get_range(kJsonKeyOrcSelectPartitionRange);

    return std::make_shared<ImportOptions>(orc_options);
  }

  Fail("Failed to create ImportOptions because JSON values are missing.");
}

std::shared_ptr<AbstractChunkReaderFactory> ImportOptions::CreateReaderFactory() const {
  switch (import_format_) {
    case ImportFormat::kCsv:
      return std::make_shared<FormatReaderFactory<CsvFormatReader>>(std::get<CsvFormatReaderOptions>(reader_options_));
    case ImportFormat::kOrc:
      return std::make_shared<FormatReaderFactory<OrcFormatReader>>(std::get<OrcFormatReaderOptions>(reader_options_));
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
