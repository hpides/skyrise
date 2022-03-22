#pragma once

#include <memory>
#include <variant>

#include <aws/core/utils/json/JsonSerializer.h>

#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/table/chunk_reader.hpp"

namespace skyrise {

enum class ImportFormat { kCsv, kOrc };

class ImportOptions {
 public:
  ImportOptions(ImportFormat import_format);
  ImportOptions(CsvFormatReaderOptions csv_format_reader_options);
  ImportOptions(OrcFormatReaderOptions orc_format_reader_options);

  /**
   * @return a FormatReaderFactory for either CSV or ORC data.
   *          The factory uses custom reader options, if provided. Otherwise, the factory is initialized with default
   *          reader options for CSV and ORC data.
   */
  const std::shared_ptr<AbstractChunkReaderFactory> CreateReaderFactory() const;

  /**
   * Serialization / Deserialization
   */
  Aws::Utils::Json::JsonValue ToJson() const;
  static std::shared_ptr<const ImportOptions> FromJson(const Aws::Utils::Json::JsonView& json);
  static Aws::Utils::Array<Aws::Utils::Json::JsonValue> TableColumnDefinitionsToJsonArray(
      const std::shared_ptr<TableColumnDefinitions>& column_definitions);
  static std::shared_ptr<TableColumnDefinitions> TableColumnDefinitionsFromJsonArray(
      const Aws::Utils::Array<Aws::Utils::Json::JsonView>& json_array);

 private:
  ImportFormat import_format_;
  std::variant<CsvFormatReaderOptions, OrcFormatReaderOptions> reader_options_;
};

}  // namespace skyrise
