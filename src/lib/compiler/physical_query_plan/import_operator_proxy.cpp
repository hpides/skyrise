#include "import_operator_proxy.hpp"

#include <magic_enum.hpp>

#include "operator/import_operator.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "utils/json.hpp"

namespace skyrise {

const std::string& ImportOperatorProxy::Name() const {
  static const auto kName = std::string{"Import"};
  return kName;
}

ImportOperatorProxy::ImportOperatorProxy(std::string bucket_name, std::vector<std::string> objects_keys,
                                         std::vector<ColumnId> column_ids, ObjectFormat format,
                                         std::shared_ptr<AbstractChunkReaderFactory> reader_factory)
    : AbstractOperatorProxy(OperatorType::kImport),
      bucket_name_(std::move(bucket_name)),
      objects_keys_(std::move(objects_keys)),
      column_ids_(std::move(column_ids)),
      format_(format),
      reader_factory_(std::move(reader_factory)) {}

std::shared_ptr<AbstractOperatorProxy> ImportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json,
                                                                     StorageFactory storage_factory) {
  Aws::String bucket_name = json.GetString("bucket_name");
  ImportOperatorProxy::ObjectFormat format = magic_enum::enum_cast<ObjectFormat>(json.GetString("format")).value();
  std::vector<std::string> object_keys = JsonArrayToVector<std::string>(json.GetArray("object_keys"));
  std::vector<ColumnId> column_ids = JsonArrayToVector<ColumnId>(json.GetArray("column_ids"));

  const auto reader_factory = [&]() -> std::shared_ptr<AbstractChunkReaderFactory> {
    switch (format) {
      case ObjectFormat::kCsv: {
        if (json.ValueExists("csv_format_reader_options")) {
          const auto& options = json.GetObject("csv_format_reader_options");

          const size_t read_buffer_size = options.GetInt64("read_buffer_size");
          const char delimiter = options.GetString("delimiter")[0];

          // GetBool() always returns false here. Therefore, boolean variables must be retrieved as objects and then
          // casted to bool
          const bool guess_delimiter = options.GetObject("guess_delimiter").AsBool();
          const bool guess_has_header = options.GetObject("guess_has_header").AsBool();
          const bool guess_has_types = options.GetObject("guess_has_types").AsBool();
          const bool has_header = options.GetObject("has_header").AsBool();
          const bool has_types = options.GetObject("has_types").AsBool();
          const auto expected_schema = ParseColumnDefinitions(options);

          const CsvFormatReaderOptions reader_options{read_buffer_size, delimiter,  guess_delimiter, guess_has_header,
                                                      guess_has_types,  has_header, has_types,       expected_schema};
          return std::make_shared<FormatReaderFactory<CsvFormatReader>>(reader_options);
        }

        return std::make_shared<FormatReaderFactory<CsvFormatReader>>();
      }
      case ObjectFormat::kOrc: {
        if (json.ValueExists("orc_format_reader_options")) {
          const auto options = json.GetObject("orc_format_reader_options");
          const bool parse_dates_as_string = options.GetBool("parse_dates_as_string");
          const auto expected_schema = ParseColumnDefinitions(options);

          const auto get_range = [&](const std::string& key) -> std::optional<std::pair<size_t, size_t>> {
            if (options.ValueExists(key)) {
              const auto row_range = options.GetObject(key);
              return std::make_pair(row_range.GetInt64("range_begin"), row_range.GetInt64("range_end"));
            }

            return std::nullopt;
          };

          const OrcFormatReaderOptions reader_options{parse_dates_as_string, expected_schema,
                                                      get_range("select_row_range"),
                                                      get_range("select_partition_range")};
          return std::make_shared<FormatReaderFactory<OrcFormatReader>>(reader_options);
        }

        return std::make_shared<FormatReaderFactory<OrcFormatReader>>();
      }
      default:
        Fail("ObjectFormat not supported.");
    }
  }();

  auto result = std::make_shared<ImportOperatorProxy>(bucket_name, object_keys, column_ids, format, reader_factory);
  if (storage_factory != nullptr) {
    result->SetStorageFactory(std::move(storage_factory));
  }

  return result;
}

Aws::Utils::Json::JsonValue ImportOperatorProxy::ToJson() const {
  auto json_output = AbstractOperatorProxy::ToJson()
                         .WithString("bucket_name", bucket_name_)
                         .WithString("format", std::string{magic_enum::enum_name(format_)})
                         .WithArray("object_keys", VectorToJsonArray(objects_keys_))
                         .WithArray("column_ids", VectorToJsonArray(column_ids_));

  switch (format_) {
    case ObjectFormat::kCsv: {
      const auto typed_reader_factory =
          std::dynamic_pointer_cast<FormatReaderFactory<CsvFormatReader>>(reader_factory_);
      const auto& configuration = typed_reader_factory->Configuration();
      auto reader_options = Aws::Utils::Json::JsonValue()
                                .WithInt64("read_buffer_size", configuration.read_buffer_size)
                                .WithString("delimiter", std::string{configuration.delimiter})
                                .WithBool("guess_delimiter", configuration.guess_delimiter)
                                .WithBool("guess_has_header", configuration.guess_has_header)
                                .WithBool("guess_has_types", configuration.guess_has_types)
                                .WithBool("has_header", configuration.has_header)
                                .WithBool("has_types", configuration.has_types);

      if (configuration.expected_schema) {
        reader_options.WithArray("expected_schema", WriteColumnDefinitions(*configuration.expected_schema));
      }

      json_output.WithObject("csv_format_reader_options", reader_options);
      break;
    }
    case ObjectFormat::kOrc: {
      const auto typed_reader_factory =
          std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory_);
      const auto& configuration = typed_reader_factory->Configuration();
      auto reader_options =
          Aws::Utils::Json::JsonValue().WithBool("parse_dates_as_string", configuration.parse_dates_as_string);

      if (configuration.expected_schema) {
        reader_options.WithArray("expected_schema", WriteColumnDefinitions(*configuration.expected_schema));
      }

      if (configuration.select_row_range.has_value()) {
        const auto& range = configuration.select_row_range.value();
        reader_options.WithObject(
            "select_row_range",
            Aws::Utils::Json::JsonValue().WithInt64("range_begin", range.first).WithInt64("range_end", range.second));
      } else if (configuration.select_partition_range.has_value()) {
        const auto& range = configuration.select_partition_range.value();
        reader_options.WithObject(
            "select_partition_range",
            Aws::Utils::Json::JsonValue().WithInt64("range_begin", range.first).WithInt64("range_end", range.second));
      }

      json_output.WithObject("orc_format_reader_options", reader_options);
      break;
    }
    default:
      break;
  }

  return json_output;
}

std::shared_ptr<AbstractOperator> ImportOperatorProxy::CreateOperatorInstance() const {
  Assert(storage_factory_ != nullptr,
         "ImportOperatorProxy expects to receive a storage factory via SetStorageFactory() or FromJson() before "
         "the operator instantiation.");
  std::shared_ptr<Storage> storage = storage_factory_(bucket_name_);

  return std::make_shared<ImportOperator>(storage, objects_keys_, column_ids_, reader_factory_);
}

std::shared_ptr<TableColumnDefinitions> ImportOperatorProxy::ParseColumnDefinitions(
    const Aws::Utils::Json::JsonView json) {
  if (!json.ValueExists("expected_schema")) {
    return nullptr;
  }

  const auto expected_schema = json.GetArray("expected_schema");
  auto definitions = std::make_shared<TableColumnDefinitions>();
  definitions->reserve(expected_schema.GetLength());

  for (size_t i = 0; i < expected_schema.GetLength(); i++) {
    definitions->emplace_back(expected_schema[i].GetString("name"),
                              magic_enum::enum_cast<DataType>(expected_schema[i].GetString("data_type")).value(),
                              expected_schema[i].GetBool("nullable"));
  }

  return definitions;
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> ImportOperatorProxy::WriteColumnDefinitions(
    const TableColumnDefinitions& definitions) {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> json_output(definitions.size());

  for (size_t i = 0; i < json_output.GetLength(); i++) {
    json_output[i] = Aws::Utils::Json::JsonValue()
                         .WithString("name", definitions[i].name)
                         .WithString("data_type", std::string{magic_enum::enum_name(definitions[i].data_type)})
                         .WithBool("nullable", definitions[i].nullable);
  }

  return json_output;
}

void ImportOperatorProxy::SetStorageFactory(StorageFactory storage_factory) {
  Assert(storage_factory != nullptr, "StorageFactory function must be provided.");
  storage_factory_ = std::move(storage_factory);
}

}  // namespace skyrise
