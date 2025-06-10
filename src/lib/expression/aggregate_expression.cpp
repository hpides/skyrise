/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "aggregate_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "aggregate_traits.hpp"
#include "all_type_variant.hpp"
#include "pqp_column_expression.hpp"
#include "utils/assert.hpp"

namespace skyrise {

AggregateExpression::AggregateExpression(const AggregateFunction aggregate_function,
                                         const std::shared_ptr<AbstractExpression>& argument)
    : AbstractExpression(ExpressionType::kAggregate, {argument}), aggregate_function_(aggregate_function) {}

std::shared_ptr<AbstractExpression> AggregateExpression::Argument() const {
  return arguments_.empty() ? nullptr : arguments_[0];
}

std::string AggregateExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;

  if (aggregate_function_ == AggregateFunction::kCountDistinct) {
    Assert(Argument(), "COUNT(DISTINCT ...) requires an argument");
    stream << "COUNT(DISTINCT " << Argument()->Description(mode) << ")";
  } else if (IsCountStar(*this)) {
    // TODO(tobodner): Add LqpColumnExpression from Hyrise for detailed output when LqpColumnExpressions are available.
    stream << "COUNT(*)";
  } else {
    stream << aggregate_function_ << "(";
    if (Argument()) {
      stream << Argument()->Description(mode);
    }
    stream << ")";
  }

  return stream.str();
}

DataType AggregateExpression::GetDataType() const {
  if (aggregate_function_ == AggregateFunction::kCount) {
    return AggregateTraits<NullValue, AggregateFunction::kCount>::kAggregateDataType;
  }

  Assert(arguments_.size() == 1, "AggregateExpression should have one argument only!");

  if (aggregate_function_ == AggregateFunction::kCountDistinct) {
    return AggregateTraits<NullValue, AggregateFunction::kCountDistinct>::kAggregateDataType;
  }

  const auto argument_data_type = Argument()->GetDataType();
  auto aggregate_data_type = DataType::kNull;
  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  ResolveDataType(argument_data_type, [&](const auto value) {
    using ColumnType = decltype(value);
    switch (aggregate_function_) {
      case AggregateFunction::kMin:
        aggregate_data_type = AggregateTraits<ColumnType, AggregateFunction::kMin>::kAggregateDataType;
        break;
      case AggregateFunction::kMax:
        aggregate_data_type = AggregateTraits<ColumnType, AggregateFunction::kMax>::kAggregateDataType;
        break;
      case AggregateFunction::kAvg:
        aggregate_data_type = AggregateTraits<ColumnType, AggregateFunction::kAvg>::kAggregateDataType;
        break;
      case AggregateFunction::kCount:
      case AggregateFunction::kCountDistinct:
        break;  // These are handled above
      case AggregateFunction::kSum:
        aggregate_data_type = AggregateTraits<ColumnType, AggregateFunction::kSum>::kAggregateDataType;
        break;
      case AggregateFunction::kStandardDeviationSample:
        aggregate_data_type =
            AggregateTraits<ColumnType, AggregateFunction::kStandardDeviationSample>::kAggregateDataType;
        break;
      case AggregateFunction::kAny:
        aggregate_data_type = AggregateTraits<ColumnType, AggregateFunction::kAny>::kAggregateDataType;
        break;
      case AggregateFunction::kStringAgg:
        aggregate_data_type = AggregateTraits<ColumnType, AggregateFunction::kStringAgg>::kAggregateDataType;
        break;
    }
  });

  return aggregate_data_type;
}

AggregateFunction AggregateExpression::GetAggregateFunction() const { return aggregate_function_; }

bool AggregateExpression::IsCountStar(const AbstractExpression& expression) {
  // COUNT(*) is represented by an AggregateExpression with the COUNT function and an kInvalidColumnId.
  if (expression.GetExpressionType() != ExpressionType::kAggregate) {
    return false;
  }
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  const auto& aggregate_expression = static_cast<const AggregateExpression&>(expression);

  if (aggregate_expression.aggregate_function_ != AggregateFunction::kCount) {
    return false;
  }

  switch (aggregate_expression.Argument()->GetExpressionType()) {
    case ExpressionType::kPqpColumn: {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      const auto& pqp_column_expression = static_cast<PqpColumnExpression&>(*aggregate_expression.Argument());
      if (pqp_column_expression.GetColumnId() != kInvalidColumnId) {
        return false;
      }
    } break;
    // TODO(tobodner) Add case ExpressionType::kLqpColumn, after LqpColumnExpressions have been added.
    default:
      Fail("Unexpected AggregateExpression argument type.");
  }

  return true;
}

bool AggregateExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const AggregateExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  return aggregate_function_ == static_cast<const AggregateExpression&>(expression).aggregate_function_;
}

size_t AggregateExpression::ShallowHash() const { return boost::hash_value(static_cast<size_t>(aggregate_function_)); }

std::shared_ptr<AbstractExpression> AggregateExpression::DeepCopy() const {
  return std::make_shared<AggregateExpression>(aggregate_function_, Argument()->DeepCopy());
}

}  // namespace skyrise
