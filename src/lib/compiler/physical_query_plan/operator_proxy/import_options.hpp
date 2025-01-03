#pragma once

#include <memory>
#include <variant>

#include <aws/core/utils/json/JsonSerializer.h>

#include "storage/formats/abstract_chunk_reader.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/formats/parquet_reader.hpp"

namespace skyrise {

enum class ImportFormat { kCsv, kOrc, kParquet };

class ImportOptions {
 public:
  explicit ImportOptions(ImportFormat object_format);
  ImportOptions(ImportFormat object_format, const std::vector<skyrise::ColumnId>& columns);
  explicit ImportOptions(CsvFormatReaderOptions csv_format_reader_options);
  explicit ImportOptions(OrcFormatReaderOptions orc_format_reader_options);
  explicit ImportOptions(ParquetFormatReaderOptions parquet_format_reader_options);
  ImportOptions(ImportFormat object_format, const std::vector<skyrise::ColumnId>& columns,
                std::optional<std::vector<int32_t>> partitions);

  /**
   * @return a FormatReaderFactory for either CSV, ORC or PARQUET data.
   *          The factory uses custom reader options, if provided. Otherwise, the factory is initialized with default
   *          reader options for CSV, ORC or PARQUET data, respectively.
   */
  std::shared_ptr<AbstractChunkReaderFactory> CreateReaderFactory() const;

  ImportFormat GetImportFormat() const { return import_format_; }

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
  std::variant<CsvFormatReaderOptions, OrcFormatReaderOptions, ParquetFormatReaderOptions> reader_options_;
};

}  // namespace skyrise
