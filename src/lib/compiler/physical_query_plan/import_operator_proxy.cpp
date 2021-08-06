#include "import_operator_proxy.hpp"

#include <magic_enum.hpp>

#include "operator/import_operator.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/table/chunk_reader.hpp"
#include "utils/json.hpp"

namespace skyrise {

const std::string& ImportOperatorProxy::Name() const {
  static const auto kName = std::string{"Import"};
  return kName;
}

ImportOperatorProxy::ImportOperatorProxy(std::string bucket_name, std::vector<std::string> objects_keys,
                                         std::vector<ColumnId> column_ids, ObjectFormat format)
    : AbstractOperatorProxy(OperatorType::kImport),
      bucket_name_(std::move(bucket_name)),
      objects_keys_(std::move(objects_keys)),
      column_ids_(std::move(column_ids)),
      format_(format) {}

std::shared_ptr<AbstractOperatorProxy> ImportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json,
                                                                     StorageFactory storage_factory) {
  Aws::String bucket_name = json.GetString("bucket_name");
  ImportOperatorProxy::ObjectFormat format = magic_enum::enum_cast<ObjectFormat>(json.GetString("format")).value();
  std::vector<std::string> object_keys = JsonArrayToVector<std::string>(json.GetArray("object_keys"));
  std::vector<ColumnId> column_ids = JsonArrayToVector<ColumnId>(json.GetArray("column_ids"));

  auto result = std::make_shared<ImportOperatorProxy>(bucket_name, object_keys, column_ids, format);
  result->SetStorageFactory(std::move(storage_factory));

  return result;
}

Aws::Utils::Json::JsonValue ImportOperatorProxy::ToJson() const {
  return AbstractOperatorProxy::ToJson()
      .WithString("bucket_name", bucket_name_)
      .WithString("format", std::string{magic_enum::enum_name(format_)})
      .WithArray("object_keys", VectorToJsonArray(objects_keys_))
      .WithArray("column_ids", VectorToJsonArray(column_ids_));
}

std::shared_ptr<AbstractOperator> ImportOperatorProxy::CreateOperatorInstance() const {
  Assert(storage_factory_ != nullptr,
         "ImportOperatorProxy expects to recieve a storage factory via SetStorageFactory() or FromJson() before "
         "the operator instantiation.");
  std::shared_ptr<Storage> storage = storage_factory_(bucket_name_);

  const auto format_reader_factory = [&]() -> std::shared_ptr<AbstractChunkReaderFactory> {
    switch (format_) {
      case ObjectFormat::kCsv:
        return std::make_shared<FormatReaderFactory<CsvFormatReader>>();
      case ObjectFormat::kOrc:
        return std::make_shared<FormatReaderFactory<OrcFormatReader>>();
    }
    Fail("Encountered invalid ObjectFormat type during ImportOperator instantiation.");
  }();

  return std::make_shared<ImportOperator>(storage, objects_keys_, column_ids_, format_reader_factory);
}

void ImportOperatorProxy::SetStorageFactory(StorageFactory storage_factory) {
  storage_factory_ = std::move(storage_factory);
}

}  // namespace skyrise
