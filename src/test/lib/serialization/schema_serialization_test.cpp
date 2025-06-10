#include "serialization/schema_serialization.hpp"

#include "gtest/gtest.h"

namespace skyrise {

class SchemaSerializationTest : public ::testing::Test {};

TEST_F(SchemaSerializationTest, SerializeTableColumnDefinition) {
  const TableColumnDefinitions definition = {
      TableColumnDefinition("id", DataType::kInt, false),
      TableColumnDefinition("name", DataType::kString, false),
      TableColumnDefinition("comment", DataType::kString, true),
      TableColumnDefinition("flag", DataType::kDouble, true),
      TableColumnDefinition("counter", DataType::kLong, true),
      TableColumnDefinition("latitude", DataType::kFloat, true),
  };
  TableColumnDefinitions deserialized_definition;

  auto buffer = std::make_shared<std::stringstream>();
  BinarySerializationStream serializer(buffer);
  serializer << definition;
  serializer >> deserialized_definition;

  EXPECT_EQ(deserialized_definition, definition);
}

}  // namespace skyrise
