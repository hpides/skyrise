#include "storage/backend/dynamodb_utils.hpp"

#include <thread>

#include <gtest/gtest.h>

#include "../test/lib/testing/aws_test.hpp"
#include "client/base_client.hpp"
#include "configuration.hpp"
#include "utils/assert.hpp"
#include "utils/literal.hpp"
#include "utils/string.hpp"
#include "utils/time.hpp"

namespace skyrise {

class AwsDynamoDbUtilsTest : public ::testing::Test {
  void SetUp() override {
    client_ = BaseClient().GetDynamoDbClient();
    table_name_ = GetUniqueName("skyriseTestTable");
    CreateDefaultTable();
  }

  void TearDown() override {
    const auto outcome = DeleteDynamoDbTable(client_, table_name_);
    Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());

    auto status = DynamoDbTableStatus(client_, table_name_);
    EXPECT_EQ(DynamoDbTableStatus(client_, table_name_), Aws::DynamoDB::Model::TableStatus::DELETING);
    while (status == Aws::DynamoDB::Model::TableStatus::DELETING) {
      std::this_thread::sleep_for(std::chrono::milliseconds(kStatePollingIntervalMilliseconds));
      status = DynamoDbTableStatus(client_, table_name_);
    }

    EXPECT_FALSE(DynamoDbTableExists(client_, table_name_));
  }

 protected:
  void CreateDefaultTable() {
    Aws::DynamoDB::Model::AttributeDefinition partition_key_definition;
    partition_key_definition.WithAttributeName(table_key_)
        .WithAttributeType(Aws::DynamoDB::Model::ScalarAttributeType::S);
    const auto outcome = CreateDynamoDbTableAndWaitForActivation(client_, table_name_, partition_key_definition);
    Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());

    EXPECT_TRUE(DynamoDbTableExists(client_, table_name_));
    EXPECT_EQ(DynamoDbTableStatus(client_, table_name_), Aws::DynamoDB::Model::TableStatus::ACTIVE);
  }

  AwsApi aws_api_;
  std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> client_;
  std::string table_name_;
  const std::string table_key_ = "id";
};

TEST_F(AwsDynamoDbUtilsTest, WriteReadDeleteSingleItem) {
  DynamoDbItem item;
  item.emplace(table_key_, "...");
  const DynamoDbItem item_key = item;
  item.emplace("body", Aws::DynamoDB::Model::AttributeValue().SetN("123"));

  const auto write_outcome = WriteDynamoDbItem(client_, table_name_, item);
  Assert(write_outcome.IsSuccess(), write_outcome.GetError().GetMessage());

  const auto read_outcome = GetDynamoDbItem(client_, table_name_, item_key);
  Assert(read_outcome.IsSuccess(), read_outcome.GetError().GetMessage());
  EXPECT_EQ(read_outcome.GetResult().GetItem(), item);

  const auto delete_outcome = DeleteDynamoDbItem(client_, table_name_, item_key);
  Assert(delete_outcome.IsSuccess(), delete_outcome.GetError().GetMessage());
}

TEST_F(AwsDynamoDbUtilsTest, ReadPartialItem) {
  DynamoDbItem item_key;
  item_key.emplace(table_key_, "...");
  DynamoDbItem item = item_key;
  item.emplace("A", "1");
  item.emplace("B", "2");
  item.emplace("C", "3");
  const auto write_outcome = WriteDynamoDbItem(client_, table_name_, item);
  Assert(write_outcome.IsSuccess(), write_outcome.GetError().GetMessage());

  auto test_partial_read = [&](const std::vector<std::string>& attributes_to_get) {
    const auto read_outcome = GetDynamoDbItem(client_, table_name_, item_key, attributes_to_get);
    Assert(read_outcome.IsSuccess(), read_outcome.GetError().GetMessage());

    const auto read_item = read_outcome.GetResult().GetItem();
    EXPECT_EQ(read_item.size(), attributes_to_get.size());
    for (const auto& attribute : attributes_to_get) {
      EXPECT_EQ(read_item.at(attribute), item.at(attribute));
    }
  };

  test_partial_read({table_key_});
  test_partial_read({"A", "C"});
  test_partial_read({"A", "C", "B"});
}

TEST_F(AwsDynamoDbUtilsTest, WriteReadBatch) {
  const size_t num_items = 111;
  std::vector<DynamoDbItem> items;
  items.reserve(num_items);
  std::vector<DynamoDbItem> item_keys;
  items.reserve(num_items);

  for (size_t i = 0; i < num_items; ++i) {
    DynamoDbItem item;
    item.emplace(table_key_, "id_" + std::to_string(i));
    item_keys.push_back(item);
    item.emplace("body", Aws::DynamoDB::Model::AttributeValue().AddNItem(std::to_string(i)));
    items.push_back(std::move(item));
  }

  const auto write_outcomes = WriteDynamoDbItems(client_, table_name_, items);
  EXPECT_EQ(write_outcomes.size(), 5);
  for (const auto& outcome : write_outcomes) {
    EXPECT_TRUE(outcome.IsSuccess());
  }

  const auto read_outcomes = GetDynamoDbItems(client_, table_name_, item_keys);

  size_t num_results = 0;
  for (const auto& outcome : read_outcomes) {
    EXPECT_TRUE(outcome.IsSuccess());
    const auto results = outcome.GetResult().GetResponses().at(table_name_);
    num_results += results.size();
  }

  EXPECT_EQ(num_results, num_items);
}

TEST_F(AwsDynamoDbUtilsTest, UpdateExistentItem) {
  DynamoDbItem item;
  item.emplace(table_key_, "...");
  const DynamoDbItem item_key = item;
  item.emplace("accessCounter", Aws::DynamoDB::Model::AttributeValue().SetN(123));
  item.emplace("description", "A");

  const auto write_outcome = WriteDynamoDbItem(client_, table_name_, item);
  Assert(write_outcome.IsSuccess(), write_outcome.GetError().GetMessage());

  DynamoDbItem update_values;
  update_values.emplace(":counterIncrease", Aws::DynamoDB::Model::AttributeValue().SetN(7));
  update_values.emplace(":newDescription", "B");

  const std::string update_expression = "ADD accessCounter :counterIncrease SET description = :newDescription";

  const auto& update_outcome = UpdateDynamoDbItem(client_, table_name_, item_key, update_values, update_expression,
                                                  std::nullopt, Aws::DynamoDB::Model::ReturnValue::ALL_NEW);
  Assert(update_outcome.IsSuccess(), update_outcome.GetError().GetMessage());

  const DynamoDbItem& result = update_outcome.GetResult().GetAttributes();
  EXPECT_EQ(std::stoi(result.at("accessCounter").GetN()), 130);
  EXPECT_EQ(result.at("description").GetS(), "B");
}

TEST_F(AwsDynamoDbUtilsTest, UpdateNonExistentItem) {
  DynamoDbItem item_key;
  item_key.emplace(table_key_, "...");

  DynamoDbItem update_values;
  update_values.emplace(":newDescription", "B");

  const std::string update_expression = "SET description = :newDescription";

  const auto& update_outcome = UpdateDynamoDbItem(client_, table_name_, item_key, update_values, update_expression);
  Assert(update_outcome.IsSuccess(), update_outcome.GetError().GetMessage());

  const auto read_outcome = GetDynamoDbItem(client_, table_name_, item_key);
  Assert(read_outcome.IsSuccess(), read_outcome.GetError().GetMessage());
  const auto result = read_outcome.GetResult().GetItem();
  EXPECT_EQ(result.at("description").GetS(), "B");
  EXPECT_EQ(result.at(table_key_).GetS(), "...");
}

TEST_F(AwsDynamoDbUtilsTest, UpdateNonExistentItemWithCondition) {
  DynamoDbItem item_key;
  item_key.emplace(table_key_, "...");

  DynamoDbItem update_values;
  update_values.emplace(":newDescription", "B");
  update_values.emplace(":id", "...");

  const std::string update_expression = "SET description = :newDescription";
  const std::string condition_expression = "id = :id";

  const auto update_outcome =
      UpdateDynamoDbItem(client_, table_name_, item_key, update_values, update_expression, condition_expression);
  EXPECT_FALSE(update_outcome.IsSuccess());
  EXPECT_EQ(update_outcome.GetError().GetErrorType(), Aws::DynamoDB::DynamoDBErrors::CONDITIONAL_CHECK_FAILED);

  WriteDynamoDbItem(client_, table_name_, item_key);
  const auto& update_outcome2 =
      UpdateDynamoDbItem(client_, table_name_, item_key, update_values, update_expression, condition_expression);
  Assert(update_outcome2.IsSuccess(), update_outcome2.GetError().GetMessage());
}

TEST_F(AwsDynamoDbUtilsTest, ScanItems) {
  const size_t num_items = 256;
  std::vector<DynamoDbItem> items;
  items.reserve(num_items);
  std::vector<DynamoDbItem> item_keys;
  items.reserve(num_items);

  const std::string data = RandomString(10_KB);

  for (size_t i = 0; i < num_items; ++i) {
    DynamoDbItem item;
    item.emplace(table_key_, "id_" + std::to_string(i));
    item_keys.push_back(item);
    item.emplace("body", data);
    items.push_back(std::move(item));
  }

  WriteDynamoDbItems(client_, table_name_, items);

  const std::string projection_expression = "id, body";
  const std::string filter_condition_expression = "begins_with(id, :prefix)";
  DynamoDbItem filter_condition_values;
  filter_condition_values.emplace(":prefix", "id_1");

  const auto& outcomes = ScanDynamoDbTable(client_, table_name_, projection_expression, filter_condition_expression,
                                           filter_condition_values);

  size_t num_results = 0;
  size_t total_items_scanned = 0;
  for (const auto& outcome : outcomes) {
    EXPECT_TRUE(outcome.IsSuccess());
    num_results += outcome.GetResult().GetItems().size();
    total_items_scanned += outcome.GetResult().GetScannedCount();
  }

  EXPECT_EQ(total_items_scanned, num_items);
  EXPECT_EQ(num_results, 111);  // 1, 10--19, and 100--199
}

TEST_F(AwsDynamoDbUtilsTest, ActivateTimeToLive) {
  const std::string ttl_attribute = "timeToLive";
  const auto outcome = ActivateTimeToLive(client_, table_name_, ttl_attribute);
  Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());

  const auto ttl_specification = outcome.GetResult().GetTimeToLiveSpecification();
  EXPECT_EQ(ttl_specification.GetAttributeName(), ttl_attribute);
  EXPECT_TRUE(ttl_specification.GetEnabled());
}

TEST(DynamoDbUtilsTest, TestErrorTranslation) {
  const std::map<StorageErrorType, std::vector<Aws::DynamoDB::DynamoDBErrors>> mapping = {
      {StorageErrorType::kInvalidArgument,
       {Aws::DynamoDB::DynamoDBErrors::INCOMPLETE_SIGNATURE, Aws::DynamoDB::DynamoDBErrors::INVALID_ACTION,
        Aws::DynamoDB::DynamoDBErrors::INVALID_PARAMETER_COMBINATION,
        Aws::DynamoDB::DynamoDBErrors::INVALID_PARAMETER_VALUE, Aws::DynamoDB::DynamoDBErrors::INVALID_QUERY_PARAMETER,
        Aws::DynamoDB::DynamoDBErrors::INVALID_SIGNATURE, Aws::DynamoDB::DynamoDBErrors::MALFORMED_QUERY_STRING,
        Aws::DynamoDB::DynamoDBErrors::MISSING_ACTION, Aws::DynamoDB::DynamoDBErrors::MISSING_PARAMETER,
        Aws::DynamoDB::DynamoDBErrors::OPT_IN_REQUIRED, Aws::DynamoDB::DynamoDBErrors::REQUEST_EXPIRED,
        Aws::DynamoDB::DynamoDBErrors::REQUEST_TIME_TOO_SKEWED}},
      {StorageErrorType::kInternalError,
       {Aws::DynamoDB::DynamoDBErrors::INTERNAL_FAILURE, Aws::DynamoDB::DynamoDBErrors::SERVICE_UNAVAILABLE}},
      {StorageErrorType::kAlreadyExist, {Aws::DynamoDB::DynamoDBErrors::TABLE_ALREADY_EXISTS}},
      {StorageErrorType::kPermissionDenied,
       {Aws::DynamoDB::DynamoDBErrors::ACCESS_DENIED, Aws::DynamoDB::DynamoDBErrors::INVALID_ACCESS_KEY_ID,
        Aws::DynamoDB::DynamoDBErrors::INVALID_CLIENT_TOKEN_ID,
        Aws::DynamoDB::DynamoDBErrors::MISSING_AUTHENTICATION_TOKEN,
        Aws::DynamoDB::DynamoDBErrors::SIGNATURE_DOES_NOT_MATCH, Aws::DynamoDB::DynamoDBErrors::UNRECOGNIZED_CLIENT,
        Aws::DynamoDB::DynamoDBErrors::VALIDATION}},
      {StorageErrorType::kTemporary,
       {Aws::DynamoDB::DynamoDBErrors::SLOW_DOWN, Aws::DynamoDB::DynamoDBErrors::THROTTLING,
        Aws::DynamoDB::DynamoDBErrors::TABLE_IN_USE}},
      {StorageErrorType::kIOError,
       {Aws::DynamoDB::DynamoDBErrors::REQUEST_TIMEOUT, Aws::DynamoDB::DynamoDBErrors::NETWORK_CONNECTION}},
      {StorageErrorType::kNotFound,
       {Aws::DynamoDB::DynamoDBErrors::RESOURCE_NOT_FOUND, Aws::DynamoDB::DynamoDBErrors::TABLE_NOT_FOUND}},
      {StorageErrorType::kUnknown, {Aws::DynamoDB::DynamoDBErrors::UNKNOWN}}};

  for (const auto& check : mapping) {
    for (const auto& error : check.second) {
      ASSERT_EQ(TranslateDynamoDbError(error), check.first);
    }
  }
}

}  // namespace skyrise
