#include "dynamodb_utils.hpp"

#include <cmath>
#include <thread>

#include <aws/dynamodb/model/BatchGetItemRequest.h>
#include <aws/dynamodb/model/BatchWriteItemRequest.h>
#include <aws/dynamodb/model/CreateTableRequest.h>
#include <aws/dynamodb/model/DeleteItemRequest.h>
#include <aws/dynamodb/model/DeleteTableRequest.h>
#include <aws/dynamodb/model/DescribeTableRequest.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <aws/dynamodb/model/KeysAndAttributes.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/ScanRequest.h>
#include <aws/dynamodb/model/UpdateItemRequest.h>
#include <aws/dynamodb/model/UpdateTimeToLiveRequest.h>
#include <magic_enum.hpp>

#include "configuration.hpp"
#include "utils/assert.hpp"

namespace skyrise {

namespace {

// These are hard limits in DynamoDB. Exceeding them will either cause a ValidationException or the entire batch
// operation is rejected.
constexpr uint kDynamoDBatchGetItemLimit = 100;
constexpr uint kDynamoDBatchWriteItemLimit = 25;

}  // namespace

Aws::DynamoDB::Model::UpdateTimeToLiveOutcome ActivateTimeToLive(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const std::string& key) {
  Aws::DynamoDB::Model::TimeToLiveSpecification ttl_specification;
  ttl_specification.WithEnabled(true).WithAttributeName(key);
  Aws::DynamoDB::Model::UpdateTimeToLiveRequest request;
  request.WithTableName(table_name).WithTimeToLiveSpecification(ttl_specification);
  return client->UpdateTimeToLive(request);
}

Aws::DynamoDB::Model::KeySchemaElement CreateKeySchemaElement(
    const Aws::DynamoDB::Model::AttributeDefinition& definition, const Aws::DynamoDB::Model::KeyType& type) {
  Aws::DynamoDB::Model::KeySchemaElement element;
  element.WithAttributeName(definition.GetAttributeName()).WithKeyType(type);
  return element;
}

Aws::DynamoDB::Model::CreateTableOutcome CreateTable(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const Aws::DynamoDB::Model::AttributeDefinition& partition_key_definition,
    const std::optional<Aws::DynamoDB::Model::AttributeDefinition>& sort_key_definition,
    const std::optional<Aws::DynamoDB::Model::StreamSpecification>& stream_specification) {
  Aws::DynamoDB::Model::CreateTableRequest request;
  request.WithTableName(table_name).WithBillingMode(Aws::DynamoDB::Model::BillingMode::PAY_PER_REQUEST);

  Aws::DynamoDB::Model::KeySchemaElement partition_key =
      CreateKeySchemaElement(partition_key_definition, Aws::DynamoDB::Model::KeyType::HASH);
  request.AddKeySchema(partition_key).AddAttributeDefinitions(partition_key_definition);

  if (sort_key_definition.has_value()) {
    Aws::DynamoDB::Model::KeySchemaElement sort_key =
        CreateKeySchemaElement(sort_key_definition.value(), Aws::DynamoDB::Model::KeyType::RANGE);

    request.AddKeySchema(sort_key).AddAttributeDefinitions(sort_key_definition.value());
  }

  if (stream_specification.has_value()) {
    request.WithStreamSpecification(stream_specification.value());
  }

  return client->CreateTable(request);
}

void WaitForActivation(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table) {
  Aws::DynamoDB::Model::TableStatus table_status = DynamoDbTableStatus(client, table);
  while (table_status == Aws::DynamoDB::Model::TableStatus::CREATING) {
    std::this_thread::sleep_for(std::chrono::milliseconds(kStatePollingIntervalMilliseconds));
    table_status = DynamoDbTableStatus(client, table);
  }
  Assert(table_status == Aws::DynamoDB::Model::TableStatus::ACTIVE,
         "Table could not be activated. It is in status: " + std::string(magic_enum::enum_name(table_status)));
}

Aws::DynamoDB::Model::CreateTableOutcome CreateDynamoDbTableAndWaitForActivation(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const Aws::DynamoDB::Model::AttributeDefinition& partition_key_definition,
    const std::optional<Aws::DynamoDB::Model::AttributeDefinition>& sort_key_definition,
    const std::optional<Aws::DynamoDB::Model::StreamSpecification>& stream_specificationn) {
  Aws::DynamoDB::Model::CreateTableOutcome outcome =
      CreateTable(client, table_name, partition_key_definition, sort_key_definition, stream_specificationn);

  if (outcome.IsSuccess()) {
    WaitForActivation(client, table_name);
  }

  return outcome;
}

Aws::DynamoDB::Model::DeleteTableOutcome DeleteDynamoDbTable(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name) {
  Aws::DynamoDB::Model::DeleteTableRequest request;
  request.SetTableName(table_name);
  return client->DeleteTable(request);
}

Aws::DynamoDB::Model::TableStatus DynamoDbTableStatus(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name) {
  Aws::DynamoDB::Model::DescribeTableRequest request;
  request.SetTableName(table_name);
  const auto outcome = client->DescribeTable(request);

  return outcome.GetResult().GetTable().GetTableStatus();
}

bool DynamoDbTableExists(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                         const std::string& table_name) {
  return DynamoDbTableStatus(client, table_name) != Aws::DynamoDB::Model::TableStatus::NOT_SET;
}

Aws::DynamoDB::Model::DeleteItemOutcome DeleteDynamoDbItem(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const DynamoDbItem& item_key) {
  Aws::DynamoDB::Model::DeleteItemRequest request;
  request.WithTableName(table_name).WithKey(item_key);
  return client->DeleteItem(request);
}

void BatchGetItem(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
                  const Aws::DynamoDB::Model::KeysAndAttributes& item_keys,
                  std::vector<Aws::DynamoDB::Model::BatchGetItemOutcome>* outcomes) {
  Aws::DynamoDB::Model::BatchGetItemRequest request;
  request.AddRequestItems(table_name, item_keys);

  const auto outcome = client->BatchGetItem(request);
  DebugAssert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  outcomes->push_back(outcome);

  // It can happen that we receive fewer items than expected.
  const auto unprocessed_items = outcome.GetResult().GetUnprocessedKeys();
  if (unprocessed_items.size() > 0) {
    BatchGetItem(client, table_name, unprocessed_items.at(table_name), outcomes);
  }
}

Aws::DynamoDB::Model::GetItemOutcome GetDynamoDbItem(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                                                     const std::string& table_name, const DynamoDbItem& item_key,
                                                     const std::vector<std::string>& attributes) {
  Aws::DynamoDB::Model::GetItemRequest request;
  request.WithTableName(table_name).WithKey(item_key);
  if (!attributes.empty()) {
    request.WithAttributesToGet(attributes);
  }
  return client->GetItem(request);
}

std::vector<Aws::DynamoDB::Model::BatchGetItemOutcome> GetDynamoDbItems(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const std::vector<DynamoDbItem>& item_keys) {
  std::vector<Aws::DynamoDB::Model::BatchGetItemOutcome> outcomes;
  outcomes.reserve(std::ceil(item_keys.size() / kDynamoDBatchGetItemLimit));

  for (size_t batch_start = 0; batch_start < item_keys.size(); batch_start += kDynamoDBatchGetItemLimit) {
    Aws::DynamoDB::Model::KeysAndAttributes keys;
    for (size_t offset = batch_start; offset < item_keys.size() && offset < (batch_start + kDynamoDBatchGetItemLimit);
         ++offset) {
      keys.AddKeys(item_keys.at(offset));
    }

    BatchGetItem(client, table_name, keys, &outcomes);
  }

  return outcomes;
}

void ScanAllItems(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                  std::vector<Aws::DynamoDB::Model::ScanOutcome>* outcomes,
                  Aws::DynamoDB::Model::ScanRequest* scan_request) {
  const Aws::DynamoDB::Model::ScanOutcome& outcome = client->Scan(*scan_request);
  outcomes->push_back(outcome);

  if (!outcome.IsSuccess()) {
    return;
  }

  const DynamoDbItem last_key = outcome.GetResult().GetLastEvaluatedKey();
  if (!last_key.empty()) {
    scan_request->SetExclusiveStartKey(last_key);
    ScanAllItems(client, outcomes, scan_request);
  }
}

std::vector<Aws::DynamoDB::Model::ScanOutcome> ScanDynamoDbTable(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const std::string& projection_expression, const std::string& filter_expression, const DynamoDbItem& filter_values) {
  std::vector<Aws::DynamoDB::Model::ScanOutcome> outcomes;

  Aws::DynamoDB::Model::ScanRequest request;
  request.WithTableName(table_name);
  if (!projection_expression.empty()) {
    request.WithProjectionExpression(projection_expression);
  }
  if (!filter_expression.empty()) {
    request.WithFilterExpression(filter_expression).WithExpressionAttributeValues(filter_values);
  }

  ScanAllItems(client, &outcomes, &request);
  return outcomes;
}

Aws::DynamoDB::Model::UpdateItemOutcome UpdateDynamoDbItem(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const DynamoDbItem& item_key, const DynamoDbItem& update_values, const std::string& update_expression,
    const std::optional<std::string>& condition_expression, const Aws::DynamoDB::Model::ReturnValue& return_type) {
  Aws::DynamoDB::Model::UpdateItemRequest request;
  request.WithTableName(table_name)
      .WithKey(item_key)
      .WithUpdateExpression(update_expression)
      .WithExpressionAttributeValues(update_values)
      .WithReturnValues(return_type);

  if (condition_expression.has_value()) {
    request.WithConditionExpression(condition_expression.value());
  }

  return client->UpdateItem(request);
}

Aws::DynamoDB::Model::PutItemOutcome WriteDynamoDbItem(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const DynamoDbItem& item) {
  Aws::DynamoDB::Model::PutItemRequest request;
  request.WithTableName(table_name).WithItem(item);
  return client->PutItem(request);
}

std::vector<Aws::DynamoDB::Model::BatchWriteItemOutcome> WriteDynamoDbItems(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const std::vector<DynamoDbItem>& items) {
  std::vector<Aws::DynamoDB::Model::BatchWriteItemOutcome> outcomes;
  outcomes.reserve(std::ceil(items.size()));

  Aws::DynamoDB::Model::WriteRequest write_request;
  Aws::DynamoDB::Model::PutRequest put_request;

  for (size_t batch_start = 0; batch_start < items.size(); batch_start += kDynamoDBatchWriteItemLimit) {
    Aws::Vector<Aws::DynamoDB::Model::WriteRequest> write_requests;
    write_requests.reserve(kDynamoDBatchWriteItemLimit);
    for (size_t offset = batch_start; offset < items.size() && offset < (batch_start + kDynamoDBatchWriteItemLimit);
         ++offset) {
      put_request.SetItem(items.at(offset));
      write_request.SetPutRequest(put_request);
      write_requests.push_back(write_request);
    }

    Aws::DynamoDB::Model::BatchWriteItemRequest request;
    request.AddRequestItems(table_name, write_requests);

    const auto outcome = client->BatchWriteItem(request);
    outcomes.push_back(outcome);
    DebugAssert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  }

  return outcomes;
}

}  // namespace skyrise
