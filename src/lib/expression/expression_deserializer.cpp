#include "expression_deserializer.hpp"

#include <magic_enum.hpp>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "utils/assert.hpp"

namespace skyrise {

namespace {

std::shared_ptr<AbstractExpression> ConstructAggregateExpression(
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(json.KeyExists("aggregate_function") && arguments.size() == 1, "Expected to find key 'aggregate_function'.");

  return std::make_shared<AggregateExpression>(
      magic_enum::enum_cast<AggregateFunction>(json.GetString("aggregate_function")).value(), std::move(arguments[0]));
}

std::shared_ptr<AbstractExpression> ConstructArithmeticExpression(
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  const auto expression_operator =
      magic_enum::enum_cast<ArithmeticOperator>(json.GetString("arithmetic_operator")).value();
  Assert(arguments.size() == 2, "Expected 2 arguments for ArithmeticExpression.");
  return std::make_shared<ArithmeticExpression>(expression_operator, std::move(arguments[0]), std::move(arguments[1]));
}

std::shared_ptr<AbstractExpression> ConstructCastExpression(
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(arguments.size() == 1, "Expected 1 argument for CastExpression.");
  const auto data_type = magic_enum::enum_cast<DataType>(json.GetString("data_type")).value();
  return std::make_shared<CastExpression>(std::move(arguments[0]), data_type);
}

std::shared_ptr<AbstractExpression> ConstructExtractExpression(
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  const auto component = magic_enum::enum_cast<DatetimeComponent>(json.GetString("datetime_component")).value();
  Assert(arguments.size() == 1, "Expected 1 argument for ExtractExpression.");
  return std::make_shared<ExtractExpression>(component, std::move(arguments[0]));
}

std::shared_ptr<AbstractExpression> ConstructListExpression(
    Aws::Utils::Json::JsonView /*json*/, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  return std::make_shared<ListExpression>(arguments);
}

std::shared_ptr<AbstractExpression> ConstructLogicalExpression(
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  const auto logical_operator = magic_enum::enum_cast<LogicalOperator>(json.GetString("logical_operator")).value();
  Assert(arguments.size() == 2, "Expected 2 arguments for LogicalExpression.");
  return std::make_shared<LogicalExpression>(logical_operator, std::move(arguments[0]), std::move(arguments[1]));
}

std::shared_ptr<AbstractExpression> ConstructPqpColumnExpression(
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(arguments.empty(), "Expected 0 arguments for PqpColumnExpression.");
  return std::make_shared<PqpColumnExpression>(json.GetInt64("column_id"),
                                               magic_enum::enum_cast<DataType>(json.GetString("data_type")).value(),
                                               json.GetInteger("is_nullable") == 1, json.GetString("column_name"));
}

std::shared_ptr<AbstractExpression> ConstructPredicateExpression(
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  const auto condition = magic_enum::enum_cast<PredicateCondition>(json.GetString("predicate_condition")).value();
  switch (condition) {
    case PredicateCondition::kEquals:
    case PredicateCondition::kNotEquals:
    case PredicateCondition::kLessThan:
    case PredicateCondition::kLessThanEquals:
    case PredicateCondition::kGreaterThan:
    case PredicateCondition::kGreaterThanEquals:
    case PredicateCondition::kLike:
    case PredicateCondition::kNotLike:
      Assert(arguments.size() == 2, "Expected 2 arguments for BinaryPredicateExpression.");
      return std::make_shared<BinaryPredicateExpression>(condition, std::move(arguments[0]), std::move(arguments[1]));

    case PredicateCondition::kBetweenInclusive:
    case PredicateCondition::kBetweenLowerExclusive:
    case PredicateCondition::kBetweenUpperExclusive:
    case PredicateCondition::kBetweenExclusive:
      Assert(arguments.size() == 3, "Expected 3 arguments for BetweenExpression.");
      return std::make_shared<BetweenExpression>(condition, std::move(arguments[0]), std::move(arguments[1]),
                                                 std::move(arguments[2]));

    case PredicateCondition::kIn:
    case PredicateCondition::kNotIn:
      Assert(arguments.size() == 2, "Expected 2 arguments for InExpression.");
      return std::make_shared<InExpression>(condition, std::move(arguments[0]), std::move(arguments[1]));

    case PredicateCondition::kIsNull:
    case PredicateCondition::kIsNotNull:
      Assert(arguments.size() == 1, "Expected 1 argument for IsNullExpression.");
      return std::make_shared<IsNullExpression>(condition, std::move(arguments[0]));
    default:
      Fail("Encountered unsupported predicate condition.");
  }
}

std::shared_ptr<AbstractExpression> ConstructUnaryMinusExpression(
    Aws::Utils::Json::JsonView /*json*/, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(arguments.size() == 1, "Expected 1 argument for UnaryMinusExpression.");
  return std::make_shared<UnaryMinusExpression>(std::move(arguments[0]));
}

std::shared_ptr<AbstractExpression> ConstructValueExpression(
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& /*arguments*/) {
  const auto type = magic_enum::enum_cast<DataType>(json.GetString("value_type")).value();
  switch (type) {
    case DataType::kString:
      return std::make_shared<ValueExpression>(static_cast<std::string>(json.GetString("value")));
    case DataType::kFloat:
      return std::make_shared<ValueExpression>(static_cast<float>(json.GetDouble("value")));
    case DataType::kDouble:
      return std::make_shared<ValueExpression>(static_cast<double>(json.GetDouble("value")));
    case DataType::kInt:
      return std::make_shared<ValueExpression>(static_cast<int32_t>(json.GetInteger("value")));
    case DataType::kLong:
      return std::make_shared<ValueExpression>(static_cast<int64_t>(json.GetInt64("value")));
    case DataType::kNull:
      return std::make_shared<ValueExpression>(kNullValue);
  }

  Fail("This code is unreachable but required to compile with gcc.");
}

}  // namespace

std::shared_ptr<AbstractExpression> ExpressionDeserializer::Deserialize(Aws::Utils::Json::JsonView json) {
  Assert(json.KeyExists("type") && magic_enum::enum_cast<ExpressionType>(json.GetString("type")).has_value(),
         "Expected to find valid value for key 'type'.");

  const auto type = magic_enum::enum_cast<ExpressionType>(json.GetString("type")).value();
  std::vector<std::shared_ptr<AbstractExpression>> arguments = DeserializeArguments(json);

  switch (type) {
    case ExpressionType::kAggregate:
      return ConstructAggregateExpression(json, std::move(arguments));
    case ExpressionType::kArithmetic:
      return ConstructArithmeticExpression(json, std::move(arguments));
    case ExpressionType::kCast:
      return ConstructCastExpression(json, std::move(arguments));
    case ExpressionType::kExtract:
      return ConstructExtractExpression(json, std::move(arguments));
    case ExpressionType::kLogical:
      return ConstructLogicalExpression(json, std::move(arguments));
    case ExpressionType::kList:
      return ConstructListExpression(json, std::move(arguments));
    case ExpressionType::kPqpColumn:
      return ConstructPqpColumnExpression(json, std::move(arguments));
    case ExpressionType::kPredicate:
      return ConstructPredicateExpression(json, std::move(arguments));
    case ExpressionType::kUnaryMinus:
      return ConstructUnaryMinusExpression(json, std::move(arguments));
    case ExpressionType::kValue:
      return ConstructValueExpression(json, std::move(arguments));
    default:
      Fail("Encountered unsupported type during deserialization.");
  }
}

std::vector<std::shared_ptr<AbstractExpression>> ExpressionDeserializer::DeserializeArguments(
    Aws::Utils::Json::JsonView json) {
  std::vector<std::shared_ptr<AbstractExpression>> arguments;
  if (!json.KeyExists("arguments")) {
    return arguments;
  }
  Aws::Utils::Array<Aws::Utils::Json::JsonView> argument_jsons = json.GetArray("arguments");
  if (argument_jsons.GetLength() == 0) {
    return arguments;
  }
  arguments.reserve(argument_jsons.GetLength());

  for (size_t i = 0; i < argument_jsons.GetLength(); ++i) {
    arguments.emplace_back(Deserialize(argument_jsons[i]));
  }

  return arguments;
}

}  // namespace skyrise
