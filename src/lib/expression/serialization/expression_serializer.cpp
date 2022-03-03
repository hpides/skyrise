#include "expression_serializer.hpp"

#include <stack>
#include <string>
#include <type_traits>
#include <variant>

#include <magic_enum.hpp>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "utils/assert.hpp"

namespace skyrise {

namespace {

void SerializeAggregateExpression(Aws::Utils::Json::JsonValue* result, const AggregateExpression& expression) {
  result->WithString("aggregate_function", std::string(magic_enum::enum_name(expression.aggregate_function_)));
}

void SerializeArithmeticExpression(Aws::Utils::Json::JsonValue* result, const ArithmeticExpression& expression) {
  result->WithString("arithmetic_operator", std::string(magic_enum::enum_name(expression.arithmetic_operator_)));
}

void SerializeCastExpression(Aws::Utils::Json::JsonValue* result, const CastExpression& expression) {
  result->WithString("data_type", std::string(magic_enum::enum_name(expression.data_type_)));
}

void SerializeExtractExpression(Aws::Utils::Json::JsonValue* result, const ExtractExpression& expression) {
  result->WithString("datetime_component", std::string(magic_enum::enum_name(expression.datetime_component_)));
}

void SerializeListExpression(Aws::Utils::Json::JsonValue* /*result*/, const ListExpression& /*expression*/) {
  // ListExpression has no special members.
}

void SerializeLogicalExpression(Aws::Utils::Json::JsonValue* result, const LogicalExpression& expression) {
  result->WithString("logical_operator", std::string(magic_enum::enum_name(expression.logical_operator_)));
}

void SerializePredicateExpression(Aws::Utils::Json::JsonValue* result, const AbstractPredicateExpression& expression) {
  result->WithString("predicate_condition", std::string(magic_enum::enum_name(expression.predicate_condition_)));
}

void SerializePqpColumnExpression(Aws::Utils::Json::JsonValue* result, const PqpColumnExpression& expression) {
  result->WithInt64("column_id", expression.column_id_)
      .WithString("data_type", std::string(magic_enum::enum_name(expression.data_type_)))
      .WithInteger("is_nullable", expression.is_nullable_ ? 1 : 0)
      .WithString("column_name", expression.column_name_);
}

void SerializeUnaryMinusExpression(Aws::Utils::Json::JsonValue* /*result*/,
                                   const UnaryMinusExpression& /*expression*/) {
  // UnaryMinusExpression has no special members.
}

void SerializeValueExpression(Aws::Utils::Json::JsonValue* result, const ValueExpression& expression) {
  const DataType variant_type = DataTypeFromAllTypeVariant(expression.value_);
  result->WithString("value_type", std::string(magic_enum::enum_name(variant_type)));

  switch (variant_type) {
    case DataType::kString:
      result->WithString("value", std::get<std::string>(expression.value_));
      break;
    case DataType::kFloat:
      result->WithDouble("value", std::get<float>(expression.value_));
      break;
    case DataType::kDouble:
      result->WithDouble("value", std::get<double>(expression.value_));
      break;
    case DataType::kInt:
      result->WithInteger("value", std::get<int32_t>(expression.value_));
      break;
    case DataType::kLong:
      result->WithInt64("value", std::get<int64_t>(expression.value_));
      break;
    case DataType::kNull:
      // "value_type" alone is sufficient in this case.
      break;
    default:
      Fail("Encountered unsupported variant type during serialization.");
  }
}

}  // namespace

Aws::Utils::Json::JsonValue ExpressionSerializer::Serialize(const AbstractExpression& expression) {
  auto result = Aws::Utils::Json::JsonValue()
                    .WithString("type", std::string(magic_enum::enum_name(expression.type_)))
                    .WithArray("arguments", SerializeArguments(expression));

  switch (expression.type_) {
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

Aws::Utils::Json::JsonValue ExpressionSerializer::Serialize(const std::shared_ptr<AbstractExpression>& expression) {
  Assert(expression, "Cannot serialize null pointer.");
  return Serialize(*expression);
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> ExpressionSerializer::SerializeArguments(
    const AbstractExpression& expression) {
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> result(expression.arguments_.size());
  for (size_t i = 0; i < expression.arguments_.size(); ++i) {
    result[i] = Serialize(*expression.arguments_[i]);
  }
  return result;
}

}  // namespace skyrise
