#include "import_operator_proxy.hpp"

#include <magic_enum.hpp>

#include "operator/import_operator.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "utils/json.hpp"

namespace {

const std::string kJsonKeyBucketName = "bucket_name";
const std::string kJsonKeyColumnIds = "column_ids";
const std::string kJsonKeyImportOptions = "import_options";
const std::string kJsonKeyObjectKeys = "object_keys";

const std::string kOrcExtension = ".orc";
const std::string kCsvExtension = ".csv";

}  // namespace

namespace skyrise {

ImportOperatorProxy::ImportOperatorProxy(std::string bucket_name, std::vector<std::string> object_keys,
                                         std::vector<ColumnId> column_ids)
    : AbstractOperatorProxy(OperatorType::kImport),
      bucket_name_(std::move(bucket_name)),
      object_keys_(std::move(object_keys)),
      column_ids_(std::move(column_ids)),
      output_objects_count_(std::numeric_limits<size_t>::max()) {
  Assert(!column_ids_.empty(), "Import must involve at least one ColumnId.");
}

const std::string& ImportOperatorProxy::Name() const {
  static const std::string kName = "Import";
  return kName;
}

std::string ImportOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << AbstractOperatorProxy::Description(mode) << separator;

  // Import details
  stream << bucket_name_ << "/";
  if (mode == DescriptionMode::kMultiLine) {
    stream << separator;
  }
  if (object_keys_.size() == 1) {
    stream << object_keys_.front();
  } else {
    stream << "{" << object_keys_.size() << " objects}";
  }
  // todo(anyone) output format ORC/CSV?

  stream << separator << "ColumnIds{" << column_ids_ << "}";
  return stream.str();
}

const std::string& ImportOperatorProxy::BucketName() const { return bucket_name_; }

const std::vector<std::string>& ImportOperatorProxy::ObjectKeys() const { return object_keys_; }

const std::vector<ColumnId>& ImportOperatorProxy::ColumnIds() const { return column_ids_; }

void ImportOperatorProxy::SetImportOptions(std::shared_ptr<const ImportOptions> import_options) {
  Assert(import_options != nullptr, "Setting the ImportOptions requires a non-null shared pointer.");
  Assert(operator_instance_ == nullptr,
         "ImportOptions must be set before an ImportOperator instance is created and cached.");
  import_options_ = std::move(import_options);
}

std::shared_ptr<const ImportOptions> ImportOperatorProxy::GetImportOptions() const { return import_options_; }

bool ImportOperatorProxy::IsPipelineBreaker() const { return false; }

size_t ImportOperatorProxy::OutputObjectsCount() const {
  Assert(!object_keys_.empty(), "ImportOperatorProxy has no object keys set.");
  return std::min(object_keys_.size(), output_objects_count_);
}

void ImportOperatorProxy::SetOutputObjectsCount(size_t output_objects_count) {
  Assert(output_objects_count >= 1, "ImportOperatorProxy must specify at least one output object.");
  output_objects_count_ = output_objects_count;
}

size_t ImportOperatorProxy::OutputColumnsCount() const { return column_ids_.size(); }

Aws::Utils::Json::JsonValue ImportOperatorProxy::ToJson() const {
  auto json_output = AbstractOperatorProxy::ToJson()
                         .WithString(kJsonKeyBucketName, bucket_name_)
                         .WithArray(kJsonKeyObjectKeys, VectorToJsonArray(object_keys_))
                         .WithArray(kJsonKeyColumnIds, VectorToJsonArray(column_ids_));

  if (import_options_ != nullptr) {
    json_output.WithObject(kJsonKeyImportOptions, import_options_->ToJson());
  }

  return json_output;
}

std::shared_ptr<AbstractOperatorProxy> ImportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const Aws::String bucket_name = json.GetString(kJsonKeyBucketName);
  const std::vector<std::string> object_keys = JsonArrayToVector<std::string>(json.GetArray(kJsonKeyObjectKeys));
  const std::vector<ColumnId> column_ids = JsonArrayToVector<ColumnId>(json.GetArray(kJsonKeyColumnIds));

  auto import_proxy = ImportOperatorProxy::Make(bucket_name, object_keys, column_ids);
  import_proxy->SetAttributesFromJson(json);

  if (json.ValueExists(kJsonKeyImportOptions)) {
    const auto deserialized_import_options = ImportOptions::FromJson(json.GetObject(kJsonKeyImportOptions));
    import_proxy->SetImportOptions(deserialized_import_options);
  }

  return import_proxy;
}

std::shared_ptr<AbstractOperatorProxy> ImportOperatorProxy::OnDeepCopy(
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_left_input*/,
    const std::shared_ptr<AbstractOperatorProxy>& /*copied_right_input*/) const {
  auto copy = ImportOperatorProxy::Make(bucket_name_, object_keys_, column_ids_);
  copy->SetOutputObjectsCount(output_objects_count_);
  if (import_options_ != nullptr) {
    copy->SetImportOptions(import_options_);
  }

  return copy;
}

std::shared_ptr<AbstractOperator> ImportOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(!bucket_name_.empty(), "ImportOperatorProxy has no bucket name.");
  Assert(!object_keys_.empty(), "ImportOperatorProxy must specify at least one object key.");
  Assert(!column_ids_.empty(), "ImportOperatorProxy must specify at least one column id.");

  // The ImportOperator requires a reader factory for its operations. It can be generated from the ImportOptions object
  // of this proxy operator. However, if ImportOptions was not set, the default ImportOptions must be used. In this
  // case, the object format must be derived from the object keys and their respective file extension.
  std::shared_ptr<AbstractChunkReaderFactory> reader_factory;
  if (import_options_ != nullptr) {
    reader_factory = import_options_->CreateReaderFactory();
  } else {
    const std::string first_object_key = object_keys_.front();
    // TODO(anyone): C++20 std::string::ends_with
    auto specifies_format = [&first_object_key](const std::string& file_extension) -> bool {
      if (first_object_key.size() <= file_extension.size()) {
        return false;
      }
      const size_t file_extension_start_pos = first_object_key.size() - file_extension.size();
      return first_object_key.find(file_extension, file_extension_start_pos) != std::string::npos;
    };

    ImportFormat import_format;
    if (specifies_format(kOrcExtension)) {
      import_format = ImportFormat::kOrc;
    } else if (specifies_format(kCsvExtension)) {
      import_format = ImportFormat::kCsv;
    } else {
      Fail("Expected object key to have either a .csv or .orc file extension.");
    }
    reader_factory = ImportOptions(import_format).CreateReaderFactory();
  }

  return std::make_shared<ImportOperator>(bucket_name_, object_keys_, column_ids_, reader_factory);
}

}  // namespace skyrise
