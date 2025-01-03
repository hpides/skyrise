#pragma once

#include <optional>

#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/ReturnValue.h>

#include "storage/backend/errors.hpp"

namespace skyrise {

using DynamoDbItem = Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>;

StorageErrorType TranslateDynamoDbError(const Aws::DynamoDB::DynamoDBErrors error);

// Table operations

/**
 * An activation can take up to one hour and returns the current TimeToLiveSpecification. Any additional
 * activation call for the same table during this one hour period results in a ValidationException.
 */
Aws::DynamoDB::Model::UpdateTimeToLiveOutcome ActivateTimeToLive(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const std::string& key);

/**
 * Creates a new table in DynamoDB without waiting for it to become active.
 */
Aws::DynamoDB::Model::CreateTableOutcome CreateDynamoDbTable(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const Aws::DynamoDB::Model::AttributeDefinition& partition_key_definition,
    const std::optional<Aws::DynamoDB::Model::AttributeDefinition>& sort_key_definition = std::nullopt,
    const std::optional<Aws::DynamoDB::Model::StreamSpecification>& stream_specification = std::nullopt);

/**
 * Creates a new table in DynamoDB and waits until the table becomes active.
 */
Aws::DynamoDB::Model::CreateTableOutcome CreateDynamoDbTableAndWaitForActivation(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const Aws::DynamoDB::Model::AttributeDefinition& partition_key_definition,
    const std::optional<Aws::DynamoDB::Model::AttributeDefinition>& sort_key_definition = std::nullopt,
    const std::optional<Aws::DynamoDB::Model::StreamSpecification>& stream_specification = std::nullopt);

Aws::DynamoDB::Model::DeleteTableOutcome DeleteDynamoDbTable(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name);

Aws::DynamoDB::Model::TableStatus DynamoDbTableStatus(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name);

bool DynamoDbTableExists(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                         const std::string& table_name);

void WaitForDynamoDbTableActivation(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                                    const std::string& table_name);

// Item operations

Aws::DynamoDB::Model::DeleteItemOutcome DeleteDynamoDbItem(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const DynamoDbItem& item_key);

/**
 * Strongly consistent reads come with some disadvantages: higher latency and costs (i.e., twice as many reading units)
 * than eventually consistent reads.
 */
Aws::DynamoDB::Model::GetItemOutcome GetDynamoDbItem(const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client,
                                                     const std::string& table_name, const DynamoDbItem& item_key,
                                                     const std::vector<std::string>& attributes = {},
                                                     const bool strongly_consistent_read = true);

std::vector<Aws::DynamoDB::Model::BatchGetItemOutcome> GetDynamoDbItems(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const std::vector<DynamoDbItem>& item_keys, const bool strongly_consistent_read = true);

std::vector<Aws::DynamoDB::Model::ScanOutcome> ScanDynamoDbTable(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const std::string& projection_expression = "", const std::string& filter_expression = "",
    const DynamoDbItem& filter_values = {}, const bool strongly_consistent_read = true);

/**
 * Updates a single item.
 *
 * Note that DynamoDB's update operation will create a new item (given key + the update value) if no item with the
 * passed key can be found in the table. If you do not want to create a new item automatically in case of absence, use
 * the @param condition_expression to provide a condition that will be evaluated by DynamoDB before updating an item
 * (e.g., "id = :id_value"). The value must be provided in the @param update_values as well. In case no item satisfies
 * the @param condition_expression and thus no update is performed, the function will return a
 * CONDITIONAL_CHECK_FAILED error outcome.
 */
Aws::DynamoDB::Model::UpdateItemOutcome UpdateDynamoDbItem(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const DynamoDbItem& item_key, const DynamoDbItem& update_values, const std::string& update_expression,
    const std::optional<std::string>& condition_expression = std::nullopt,
    const Aws::DynamoDB::Model::ReturnValue& return_type = Aws::DynamoDB::Model::ReturnValue::NONE);

Aws::DynamoDB::Model::PutItemOutcome WriteDynamoDbItem(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const DynamoDbItem& item);

std::vector<Aws::DynamoDB::Model::BatchWriteItemOutcome> WriteDynamoDbItems(
    const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient>& client, const std::string& table_name,
    const std::vector<DynamoDbItem>& items);

}  // namespace skyrise
