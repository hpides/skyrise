#include "dynamodb_storage.hpp"

#include <chrono>

#include "configuration.hpp"
#include "constants.hpp"
#include "dynamodb_utils.hpp"
#include "utils/assert.hpp"

namespace skyrise {

namespace {

const std::string kItemBodyAttribute = "body";
const std::string kItemChecksumAttribute = "checksum";
const std::string kItemIdentifierAttribute = "id";
const std::string kItemLastModifiedAttribute = "lastModified";
const std::string kItemSizeAttribute = "size";

template <typename Result, typename Error>
StorageError GetErrorFromOutcome(const Aws::Utils::Outcome<Result, Error>& outcome) {
  const auto& error = outcome.GetError();
  return StorageError(TranslateDynamoDbError(error.GetErrorType()), error.GetMessage());
}

ObjectStatus GetObjectStatusFromOutcome(const Aws::DynamoDB::Model::GetItemOutcome& outcome) {
  if (outcome.IsSuccess()) {
    auto item = outcome.GetResult().GetItem();
    if (item.empty()) {
      return ObjectStatus(StorageError(StorageErrorType::kNotFound));
    }

    auto extract_required_value = [&item](const std::string& key) -> Aws::DynamoDB::Model::AttributeValue {
      const auto potential_match = item.find(key);
      Assert(potential_match != item.end(), "Required key does not exist in item.");
      return potential_match->second;
    };

    const std::string id = extract_required_value(kItemIdentifierAttribute).GetS();
    const time_t last_modified = std::stoi(extract_required_value(kItemLastModifiedAttribute).GetN());
    const std::string etag = extract_required_value(kItemChecksumAttribute).GetS();
    const size_t size = std::stoul(extract_required_value(kItemSizeAttribute).GetS());

    return {id, last_modified, etag, size};
  } else {
    return ObjectStatus(GetErrorFromOutcome(outcome));
  }
};

}  // namespace

DynamoDbObjectWriter::DynamoDbObjectWriter(std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client,
                                           const std::string& table_name, const std::string& object_id)
    : client_(std::move(client)), table_name_(table_name), object_id_(object_id) {}

StorageError DynamoDbObjectWriter::Write(const char* data, size_t length) {
  if (closed_) {
    return StorageError(StorageErrorType::kInvalidState);
  }

  buffer_.append(data, length);

  return (buffer_.size() + kDynamoDbStorageMetadataSizeBytes) <= kDynamoDbMaxItemSizeBytes
             ? StorageError::Success()
             : StorageError(StorageErrorType::kInvalidArgument, "Data size exceeds DynamoDB's maximum item size.");
}

StorageError DynamoDbObjectWriter::Close() {
  if (!closed_) {
    closed_ = true;
    return UploadBuffer();
  }

  return StorageError::Success();
}

StorageError DynamoDbObjectWriter::UploadBuffer() {
  DynamoDbItem item_key;
  item_key.emplace(kItemIdentifierAttribute, object_id_);

  DynamoDbItem new_item_body;
  std::stringstream new_body_update_expression;
  new_body_update_expression << "SET ";

  const auto add_attribute = [&](const std::string& attribute_key, const Aws::DynamoDB::Model::AttributeValue& value) {
    const auto value_key = ":" + attribute_key;
    new_item_body.emplace(value_key, value);
    new_body_update_expression << attribute_key << "=" << value_key << ", ";
  };

  const int last_modified =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  const std::string checksum = std::to_string(std::hash<std::string>{}(buffer_));
  const size_t size = buffer_.size();

  add_attribute(kItemLastModifiedAttribute, Aws::DynamoDB::Model::AttributeValue().SetN(last_modified));
  add_attribute(kItemChecksumAttribute, Aws::DynamoDB::Model::AttributeValue(checksum));
  add_attribute(kItemSizeAttribute, Aws::DynamoDB::Model::AttributeValue(std::to_string(size)));
  add_attribute(kItemBodyAttribute, Aws::DynamoDB::Model::AttributeValue(buffer_));

  auto new_body_update_expression_string = new_body_update_expression.str();
  new_body_update_expression_string.resize(new_body_update_expression_string.size() - 2);

  const auto outcome =
      UpdateDynamoDbItem(client_, table_name_, item_key, new_item_body, new_body_update_expression_string, std::nullopt,
                         Aws::DynamoDB::Model::ReturnValue::NONE);

  buffer_.clear();
  buffer_.shrink_to_fit();

  return outcome.IsSuccess() ? StorageError::Success() : GetErrorFromOutcome(outcome);
}

DynamoDbStorage::DynamoDbStorage(std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client, std::string table_name)
    : client_(std::move(client)), table_name_(std::move(table_name)) {}

StorageError DynamoDbStorage::CreateTable(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                                          const std::string& table) {
  Aws::DynamoDB::Model::AttributeDefinition partition_key_definition;
  partition_key_definition.WithAttributeName(kItemIdentifierAttribute)
      .WithAttributeType(Aws::DynamoDB::Model::ScalarAttributeType::S);

  const auto outcome = CreateDynamoDbTableAndWaitForActivation(client, table, partition_key_definition);
  return outcome.IsSuccess() ? StorageError::Success() : GetErrorFromOutcome(outcome);
}

StorageError DynamoDbStorage::DeleteTable(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                                          const std::string& table_name) {
  const auto outcome = DeleteDynamoDbTable(client, table_name);
  return outcome.IsSuccess() ? StorageError::Success() : GetErrorFromOutcome(outcome);
}

StorageError DynamoDbStorage::Delete(const std::string& object_identifier) {
  DynamoDbItem item_key;
  item_key.emplace(kItemIdentifierAttribute, object_identifier);

  const auto outcome = DeleteDynamoDbItem(client_, table_name_, item_key);
  return outcome.IsSuccess() ? StorageError::Success() : GetErrorFromOutcome(outcome);
}

std::pair<std::vector<ObjectStatus>, StorageError> DynamoDbStorage::List(const std::string& object_prefix) {
  std::stringstream projection_expression;
  projection_expression << kItemIdentifierAttribute << ", " << kItemChecksumAttribute << ", " << kItemSizeAttribute
                        << ", " << kItemLastModifiedAttribute;

  std::stringstream filter_expression;
  filter_expression << "begins_with(" << kItemIdentifierAttribute << ", :" << kItemIdentifierAttribute << ")";

  DynamoDbItem filter_values;
  filter_values.emplace(":" + kItemIdentifierAttribute, object_prefix);

  const std::vector<Aws::DynamoDB::Model::ScanOutcome> outcomes = ScanDynamoDbTable(
      client_, table_name_, projection_expression.str(), filter_expression.str(), filter_values, true);

  StorageError error = StorageError::Success();
  std::vector<ObjectStatus> result_vector;
  for (const auto& outcome : outcomes) {
    if (outcome.IsSuccess()) {
      const auto& items = outcome.GetResult().GetItems();
      result_vector.reserve(result_vector.size() + items.size());
      for (const auto& item : items) {
        const std::string id = item.at(kItemIdentifierAttribute).GetS();
        const time_t last_modified = std::stoi(item.at(kItemLastModifiedAttribute).GetN());
        const std::string etag = item.at(kItemChecksumAttribute).GetS();
        const size_t size = std::stoul(item.at(kItemSizeAttribute).GetS());

        result_vector.emplace_back(id, last_modified, etag, size);
      }
    } else {
      error = GetErrorFromOutcome(outcome);
      break;
    }
  }

  return std::make_pair(result_vector, error);
}

DynamoDbObjectReader::DynamoDbObjectReader(std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client,
                                           std::string table_name, std::string object_id)
    : client_(std::move(client)), table_name_(std::move(table_name)), object_id_(std::move(object_id)) {}

StorageError DynamoDbObjectReader::Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) {
  DynamoDbItem item_key;
  item_key.emplace(kItemIdentifierAttribute, object_id_);

  const auto outcome = GetDynamoDbItem(client_, table_name_, item_key, {}, true);

  if (!outcome.IsSuccess()) {
    return GetErrorFromOutcome(outcome);
  }

  // If we do not have status information about the object, we can obtain it now.
  if (status_.GetError().IsError()) {
    status_ = GetObjectStatusFromOutcome(outcome);
  }

  const auto data = outcome.GetResult().GetItem().at(kItemBodyAttribute).GetS();

  Assert(first_byte < data.size(), "First byte read is out of bounds.");
  const auto start = data.cbegin() + first_byte;

  last_byte = std::min(last_byte, data.size() - 1);
  const size_t read_length = last_byte - first_byte + 1;

  buffer->Resize(read_length);
  std::copy_n(start, read_length, buffer->CharData());

  return StorageError::Success();
}

const ObjectStatus& DynamoDbObjectReader::GetStatus() {
  if (status_.GetError().IsError()) {
    DynamoDbItem item_key;
    item_key.emplace(kItemIdentifierAttribute, object_id_);
    const std::vector<std::string> attributes_to_get = {kItemChecksumAttribute, kItemIdentifierAttribute,
                                                        kItemLastModifiedAttribute, kItemSizeAttribute};

    const auto outcome = GetDynamoDbItem(client_, table_name_, item_key, attributes_to_get, true);
    status_ = GetObjectStatusFromOutcome(outcome);
  }
  return status_;
}

StorageError DynamoDbObjectReader::Close() { return StorageError::Success(); }

}  // namespace skyrise
