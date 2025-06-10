#include "expression_serialization.hpp"

#include <string>
#include <variant>

#include <magic_enum/magic_enum.hpp>

#include "case_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/cast_expression.hpp"
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

/**
 * Expression Serialization
 */

Aws::Utils::Array<Aws::Utils::Json::JsonValue> SerializeArguments(const AbstractExpression& expression) {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> result(expression.GetArguments().size());
  for (size_t i = 0; i < expression.GetArguments().size(); ++i) {
    result[i] = SerializeExpression(*expression.GetArguments()[i]);
  }
  return result;
}

void SerializeAggregateExpression(Aws::Utils::Json::JsonValue* result, const AggregateExpression& expression) {
  result->WithString("aggregate_function", std::string(magic_enum::enum_name(expression.GetAggregateFunction())));
}

void SerializeArithmeticExpression(Aws::Utils::Json::JsonValue* result, const ArithmeticExpression& expression) {
  result->WithString("arithmetic_operator", std::string(magic_enum::enum_name(expression.GetArithmeticOperator())));
}

void SerializeCaseExpression(Aws::Utils::Json::JsonValue* /*result*/, const CaseExpression& /*expression*/) {
  // CaseExpression has no special members.
}

void SerializeCastExpression(Aws::Utils::Json::JsonValue* result, const CastExpression& expression) {
  result->WithString("data_type", std::string(magic_enum::enum_name(expression.GetDataType())));
}

void SerializeExtractExpression(Aws::Utils::Json::JsonValue* result, const ExtractExpression& expression) {
  result->WithString("datetime_component", std::string(magic_enum::enum_name(expression.GetDatetimeComponent())));
}

void SerializeListExpression(Aws::Utils::Json::JsonValue* /*result*/, const ListExpression& /*expression*/) {
  // ListExpression has no special members.
}

void SerializeLogicalExpression(Aws::Utils::Json::JsonValue* result, const LogicalExpression& expression) {
  result->WithString("logical_operator", std::string(magic_enum::enum_name(expression.GetLogicalOperator())));
}

void SerializePredicateExpression(Aws::Utils::Json::JsonValue* result, const AbstractPredicateExpression& expression) {
  result->WithString("predicate_condition", std::string(magic_enum::enum_name(expression.GetPredicateCondition())));
}

void SerializePqpColumnExpression(Aws::Utils::Json::JsonValue* result, const PqpColumnExpression& expression) {
  result->WithInt64("column_id", expression.GetColumnId())
      .WithString("data_type", std::string(magic_enum::enum_name(expression.GetDataType())))
      .WithInteger("is_nullable", expression.IsNullable() ? 1 : 0)
      .WithString("column_name", expression.GetColumnName());
}

void SerializeUnaryMinusExpression(Aws::Utils::Json::JsonValue* /*result*/,
                                   const UnaryMinusExpression& /*expression*/) {
  // UnaryMinusExpression has no special members.
}

void SerializeValueExpression(Aws::Utils::Json::JsonValue* result, const ValueExpression& expression) {
  const DataType variant_type = DataTypeFromAllTypeVariant(expression.GetValue());
  result->WithString("value_type", std::string(magic_enum::enum_name(variant_type)));

  switch (variant_type) {
    case DataType::kString:
      result->WithString("value", std::get<std::string>(expression.GetValue()));
      break;
    case DataType::kFloat:
      result->WithDouble("value", std::get<float>(expression.GetValue()));
      break;
    case DataType::kDouble:
      result->WithDouble("value", std::get<double>(expression.GetValue()));
      break;
    case DataType::kInt:
      result->WithInteger("value", std::get<int32_t>(expression.GetValue()));
      break;
    case DataType::kLong:
      result->WithInt64("value", std::get<int64_t>(expression.GetValue()));
      break;
    case DataType::kNull:
      // "value_type" alone is sufficient in this case.
      break;
    default:
      Fail("Encountered unsupported variant type during serialization.");
  }
}

/**
 * Expression Deserialization
 */

std::vector<std::shared_ptr<AbstractExpression>> DeserializeExpressionArguments(Aws::Utils::Json::JsonView json) {
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
    arguments.emplace_back(DeserializeExpression(argument_jsons[i]));
  }

  return arguments;
}

std::shared_ptr<AbstractExpression> ConstructAggregateExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(json.KeyExists("aggregate_function") && arguments.size() == 1, "Expected to find key 'aggregate_function'.");
  return std::make_shared<AggregateExpression>(
      magic_enum::enum_cast<AggregateFunction>(json.GetString("aggregate_function")).value(), std::move(arguments[0]));
}

std::shared_ptr<AbstractExpression> ConstructArithmeticExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  const auto expression_operator =
      magic_enum::enum_cast<ArithmeticOperator>(json.GetString("arithmetic_operator")).value();
  Assert(arguments.size() == 2, "Expected 2 arguments for ArithmeticExpression.");
  return std::make_shared<ArithmeticExpression>(expression_operator, std::move(arguments[0]), std::move(arguments[1]));
}

std::shared_ptr<AbstractExpression> ConstructCaseExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView /*json*/, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(arguments.size() == 3, "Expected 3 argument for CaseExpression.");
  return std::make_shared<CaseExpression>(std::move(arguments[0]), std::move(arguments[1]), std::move(arguments[2]));
}

std::shared_ptr<AbstractExpression> ConstructCastExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(arguments.size() == 1, "Expected 1 argument for CastExpression.");
  const auto data_type = magic_enum::enum_cast<DataType>(json.GetString("data_type")).value();
  return std::make_shared<CastExpression>(std::move(arguments[0]), data_type);
}

std::shared_ptr<AbstractExpression> ConstructExtractExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  const auto component = magic_enum::enum_cast<DatetimeComponent>(json.GetString("datetime_component")).value();
  Assert(arguments.size() == 1, "Expected 1 argument for ExtractExpression.");
  return std::make_shared<ExtractExpression>(component, std::move(arguments[0]));
}

std::shared_ptr<AbstractExpression> ConstructListExpression(
    Aws::Utils::Json::JsonView /*json*/, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  return std::make_shared<ListExpression>(std::move(arguments));
}

std::shared_ptr<AbstractExpression> ConstructLogicalExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  const auto logical_operator = magic_enum::enum_cast<LogicalOperator>(json.GetString("logical_operator")).value();
  Assert(arguments.size() == 2, "Expected 2 arguments for LogicalExpression.");
  return std::make_shared<LogicalExpression>(logical_operator, std::move(arguments[0]), std::move(arguments[1]));
}

std::shared_ptr<AbstractExpression> ConstructPqpColumnExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(arguments.empty(), "Expected 0 arguments for PqpColumnExpression.");
  return std::make_shared<PqpColumnExpression>(json.GetInt64("column_id"),
                                               magic_enum::enum_cast<DataType>(json.GetString("data_type")).value(),
                                               json.GetInteger("is_nullable") == 1, json.GetString("column_name"));
}

std::shared_ptr<AbstractExpression> ConstructPredicateExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
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
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView /*json*/, std::vector<std::shared_ptr<AbstractExpression>>&& arguments) {
  Assert(arguments.size() == 1, "Expected 1 argument for UnaryMinusExpression.");
  return std::make_shared<UnaryMinusExpression>(std::move(arguments[0]));
}

std::shared_ptr<AbstractExpression> ConstructValueExpression(
    // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
    Aws::Utils::Json::JsonView json, std::vector<std::shared_ptr<AbstractExpression>>&& /*arguments*/) {
  const auto type = magic_enum::enum_cast<DataType>(json.GetString("value_type")).value();
  switch (type) {
    case DataType::kString:
      return std::make_shared<ValueExpression>(static_cast<std::string>(json.GetString("value")));
    case DataType::kFloat:
      return std::make_shared<ValueExpression>(static_cast<float>(json.GetDouble("value")));
    case DataType::kDouble:
      return std::make_shared<ValueExpression>(json.GetDouble("value"));
    case DataType::kInt:
      return std::make_shared<ValueExpression>(static_cast<int32_t>(json.GetInteger("value")));
    case DataType::kLong:
      return std::make_shared<ValueExpression>(json.GetInt64("value"));
    case DataType::kNull:
      return std::make_shared<ValueExpression>(kNullValue);
  }

  Fail("This code is unreachable but required to compile with gcc.");
}

}  // namespace

Aws::Utils::Json::JsonValue SerializeExpression(const AbstractExpression& expression) {
  auto result = Aws::Utils::Json::JsonValue()
                    .WithString("type", std::string(magic_enum::enum_name(expression.GetExpressionType())))
                    .WithArray("arguments", SerializeArguments(expression));

  switch (expression.GetExpressionType()) {
    case ExpressionType::kValue:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeValueExpression(&result, static_cast<const ValueExpression&>(expression));
      break;
    case ExpressionType::kPredicate:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializePredicateExpression(&result, static_cast<const AbstractPredicateExpression&>(expression));
      break;
    case ExpressionType::kArithmetic:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeArithmeticExpression(&result, static_cast<const ArithmeticExpression&>(expression));
      break;
    case ExpressionType::kCase:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeCaseExpression(&result, static_cast<const CaseExpression&>(expression));
      break;
    case ExpressionType::kCast:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeCastExpression(&result, static_cast<const CastExpression&>(expression));
      break;
    case ExpressionType::kExtract:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeExtractExpression(&result, static_cast<const ExtractExpression&>(expression));
      break;
    case ExpressionType::kList:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeListExpression(&result, static_cast<const ListExpression&>(expression));
      break;
    case ExpressionType::kLogical:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeLogicalExpression(&result, static_cast<const LogicalExpression&>(expression));
      break;
    case ExpressionType::kPqpColumn:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializePqpColumnExpression(&result, static_cast<const PqpColumnExpression&>(expression));
      break;
    case ExpressionType::kUnaryMinus:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeUnaryMinusExpression(&result, static_cast<const UnaryMinusExpression&>(expression));
      break;
    case ExpressionType::kAggregate:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      SerializeAggregateExpression(&result, static_cast<const AggregateExpression&>(expression));
      break;
    default:
      Fail("Encountered unsupported type during serialization.");
  }

  return result;
}

Aws::Utils::Json::JsonValue SerializeExpression(const std::shared_ptr<AbstractExpression>& expression) {
  Assert(expression, "Cannot serialize null pointer.");
  return SerializeExpression(*expression);
}

std::shared_ptr<AbstractExpression> DeserializeExpression(Aws::Utils::Json::JsonView json) {
  Assert(json.KeyExists("type") && magic_enum::enum_cast<ExpressionType>(json.GetString("type")).has_value(),
         "Expected to find valid value for key 'type'.");

  const auto type = magic_enum::enum_cast<ExpressionType>(json.GetString("type")).value();
  std::vector<std::shared_ptr<AbstractExpression>> arguments = DeserializeExpressionArguments(json);

  switch (type) {
    case ExpressionType::kAggregate:
      return ConstructAggregateExpression(json, std::move(arguments));
    case ExpressionType::kArithmetic:
      return ConstructArithmeticExpression(json, std::move(arguments));
    case ExpressionType::kCase:
      return ConstructCaseExpression(json, std::move(arguments));
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

}  // namespace skyrise
