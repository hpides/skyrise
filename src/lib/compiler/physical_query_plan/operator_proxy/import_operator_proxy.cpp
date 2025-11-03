#include "import_operator_proxy.hpp"

#include <boost/container_hash/hash.hpp>
#include <magic_enum.hpp>

#include "configuration.hpp"
#include "constants.hpp"
#include "operator/import_operator.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "utils/json.hpp"

namespace {

const std::string kName = "Import";
const std::string kJsonKeyImportOptions = "import_options";
const std::string kJsonKeyColumnIds = "column_ids";

const std::string kJsonKeyObjectReferences = "import_references";
const std::string kJsonKeyBucketName = "bucket";
const std::string kJsonKeyObjectKey = "key";
const std::string kJsonKeyObjectEtag = "etag";

}  // namespace

namespace skyrise {

ImportOperatorProxy::ImportOperatorProxy(std::vector<ObjectReference> object_references,
                                         std::vector<ColumnId> column_ids)
    : AbstractOperatorProxy(OperatorType::kImport),
      column_ids_(std::move(column_ids)),
      object_references_(std::move(object_references)) {
  Assert(!column_ids_.empty(), "ImportOperatorProxy must specify at least one import ColumnId.");
  output_data_traits_.column_count = column_ids_.size();

  // We aim for maximum data parallelism by default. Ideally, this means that objects are processed in separate buckets,
  // meaning by individual Lambda function workers.
  object_to_bucket_strategy_ = ObjectToBucketStrategy::MultipleBuckets;
  // Data parallelism, however, is limited by the burst concurrency quota of the AWS Lambda service.
  output_data_traits_.bucket_count = std::min(object_references_.size(), kLambdaFunctionConcurrencyLimit);
  output_data_traits_.partition_count = 1;
}

const std::string& ImportOperatorProxy::Name() const { return kName; }

std::string ImportOperatorProxy::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  const char separator = mode == DescriptionMode::kSingleLine ? ' ' : '\n';
  stream << AbstractOperatorProxy::Description(mode) << separator;

  // Import details
  stream << object_references_.front().bucket_name << "/";
  if (mode == DescriptionMode::kMultiLine) {
    stream << separator;
  }
  if (object_references_.size() == 1) {
    stream << object_references_.front().identifier;
  } else {
    stream << "{" << object_references_.size() << " objects}";
  }

  stream << separator << "ColumnIds{" << column_ids_ << "}";

  if (!origin_identifier_) {
    return stream.str();
  }

  // Origin details
  stream << separator;
  if (mode == DescriptionMode::kSingleLine) {
    stream << "- ";
  }
  stream << "Origin: " << *origin_identifier_;

  return stream.str();
}

const std::vector<ColumnId>& ImportOperatorProxy::ColumnIds() const { return column_ids_; }

const std::vector<ObjectReference>& ImportOperatorProxy::ObjectReferences() const { return object_references_; }

void ImportOperatorProxy::SetObjectReferences(std::vector<ObjectReference> object_references) {
  output_data_traits_.bucket_count = object_references.size();
  object_references_ = std::move(object_references);
}

std::shared_ptr<const ImportOptions> ImportOperatorProxy::GetImportOptions() const { return import_options_; }

void ImportOperatorProxy::SetImportOptions(std::shared_ptr<const ImportOptions> import_options) {
  Assert(import_options != nullptr, "Setting the ImportOptions requires a non-null shared pointer.");
  Assert(operator_instance_ == nullptr,
         "ImportOptions must be set before an ImportOperator instance is created and cached.");
  import_options_ = std::move(import_options);
}

const DataTraits& ImportOperatorProxy::OutputDataTraits() const {
  Assert(output_data_traits_.column_count == column_ids_.size(), "Invalid column count in OutputDataTraits.");
  Assert(output_data_traits_.bucket_count <= kLambdaFunctionConcurrencyLimit,
         "Data parallelism exceeds burst concurrency quota of AWS Lambda.");
  if (object_to_bucket_strategy_ == ObjectToBucketStrategy::PartitionedBuckets) {
    output_data_traits_.partition_count = expected_partition_count_;
  } else {
    output_data_traits_.partition_count = 1;
  }
  return output_data_traits_;
}

bool ImportOperatorProxy::IsPipelineBreaker() const { return false; }

const std::string& ImportOperatorProxy::OriginIdentifier() const { return origin_identifier_; }

void ImportOperatorProxy::SetOriginTraits(const std::string& origin_identifier, const size_t partition_count) {
  Assert(origin_identifier.empty(), "Empty origin identifier.");
  origin_identifier_ = origin_identifier;
  expected_partition_count_ = partition_count;
}

ObjectToBucketStrategy ImportOperatorProxy::GetObjectToBucketStrategy() const { return object_to_bucket_strategy_; }

void ImportOperatorProxy::SetObjectToBucketStrategy(ObjectToBucketStrategy object_to_bucket_strategy,
                                                    size_t bucket_count) {
  Assert(object_to_bucket_strategy == ObjectToBucketStrategy::SingleBucket || bucket_count > 1,
         "Invalid bucket_count for given ObjectToBucketStrategy.");
  Assert(bucket_count <= object_references_.size(), "Bucket count cannot be greater than count of objects to import.");
  object_to_bucket_strategy_ = object_to_bucket_strategy;
  output_data_traits_.bucket_count = bucket_count;

  if (object_to_bucket_strategy_ == ObjectToBucketStrategy::PartitionedBuckets) {
    Assert()
  }
}

Aws::Utils::Json::JsonValue ImportOperatorProxy::ToJson() const {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> object_references_array(object_references_.size());
  for (size_t i = 0; i < object_references_.size(); ++i) {
    const auto& object_reference = object_references_[i];
    object_references_array[i]
        .WithString(kJsonKeyBucketName, object_reference.bucket_name)
        .WithString(kJsonKeyObjectKey, object_reference.identifier)
        .WithString(kJsonKeyObjectEtag, object_reference.etag);
  }

  auto json_output = AbstractOperatorProxy::ToJson()
                         .WithArray(kJsonKeyColumnIds, VectorToJsonArray(column_ids_))
                         .WithArray(kJsonKeyObjectReferences, object_references_array);

  if (import_options_ != nullptr) {
    json_output.WithObject(kJsonKeyImportOptions, import_options_->ToJson());
  }

  // The following attributes do not get serialized because they are relevant for query execution, and thus must not be
  // transferred to cloud function workers, for instance.
  //  - object_to_bucket_strategy_
  //  - output_data_traits_
  //  - origin_identifier_
  //  - expected_partition_count_

  return json_output;
}

std::shared_ptr<AbstractOperatorProxy> ImportOperatorProxy::FromJson(const Aws::Utils::Json::JsonView& json) {
  const auto column_ids = JsonArrayToVector<ColumnId>(json.GetArray(kJsonKeyColumnIds));
  const auto object_references_json_array = json.GetArray(kJsonKeyObjectReferences);
  std::vector<ObjectReference> objects_references;
  objects_references.reserve(object_references_json_array.GetLength());
  for (size_t i = 0; i < object_references_json_array.GetLength(); ++i) {
    const auto serialized_obect_reference = object_references_json_array.GetItem(i);
    objects_references.emplace_back(serialized_obect_reference.GetString(kJsonKeyBucketName),
                                    serialized_obect_reference.GetString(kJsonKeyObjectKey),
                                    serialized_obect_reference.GetString(kJsonKeyObjectEtag));
  }

  auto import_proxy = ImportOperatorProxy::Make(objects_references, column_ids);
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
  auto copy = ImportOperatorProxy::Make(object_references_, column_ids_);
  copy->import_options_ = import_options_;

  copy->object_to_bucket_strategy_ = object_to_bucket_strategy_;
  copy->output_data_traits_ = output_data_traits_;
  copy->origin_identifier_ = origin_identifier_;
  copy->expected_partition_count_ = expected_partition_count_;

  return copy;
}

size_t ImportOperatorProxy::ShallowHash() const {
  size_t hash = 0;
  for (const auto& object_reference : object_references_) {
    boost::hash_combine(hash, object_reference.bucket_name);
    boost::hash_combine(hash, object_reference.identifier);
    boost::hash_combine(hash, object_reference.etag);
  }

  for (const auto column_id : column_ids_) {
    boost::hash_combine(hash, column_id);
  }

  if (import_options_) {
    // TODO(anyone): Do we want to hash ImportOptions attribute-by-attribute?
    boost::hash_combine(hash, import_options_);
  }

  boost::hash_combine(hash, object_to_bucket_strategy_);
  boost::hash_combine(hash, output_data_traits_.bucket_count);
  boost::hash_combine(hash, output_data_traits_.partition_count);
  boost::hash_combine(hash, *origin_identifier_);
  boost::hash_combine(hash, expected_partition_count_);

  return hash;
}

std::shared_ptr<AbstractOperator> ImportOperatorProxy::CreateOperatorInstanceRecursively() {
  Assert(!object_references_.empty(), "ImportOperatorProxy must specify at least one object.");
  Assert(!column_ids_.empty(), "ImportOperatorProxy must specify at least one column id.");

  // The ImportOperator requires a reader factory for its operations. It can be generated from the ImportOptions object
  // of this proxy operator. However, if ImportOptions was not set, the default ImportOptions must be used. In this
  // case, the object format must be derived from the object keys and their respective file extension.
  std::shared_ptr<AbstractChunkReaderFactory> reader_factory;
  if (import_options_ != nullptr) {
    reader_factory = import_options_->CreateReaderFactory();
  } else {
    const std::string first_object_key = object_references_.front().identifier;
    auto specifies_format = [&first_object_key](const std::string& file_extension) -> bool {
      if (first_object_key.size() <= file_extension.size()) {
        return false;
      }
      const size_t file_extension_start_pos = first_object_key.size() - file_extension.size();
      return first_object_key.find(file_extension, file_extension_start_pos) != std::string::npos;
    };

    ImportFormat import_format = ImportFormat::kCsv;
    if (specifies_format(std::string(kOrcExtension))) {
      import_format = ImportFormat::kOrc;
    } else if (specifies_format(std::string(kCsvExtension))) {
      import_format = ImportFormat::kCsv;
    } else {
      Fail("Expected object key to have either a .csv or .orc file extension.");
    }
    reader_factory = ImportOptions(import_format, column_ids_).CreateReaderFactory();
  }

  return std::make_shared<ImportOperator>(object_references_, column_ids_, reader_factory);
}

}  // namespace skyrise
