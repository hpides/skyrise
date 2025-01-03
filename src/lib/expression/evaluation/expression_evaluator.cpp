/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "expression_evaluator.hpp"

#include <iterator>
#include <type_traits>

#include <boost/lexical_cast.hpp>

#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/expression_functional.hpp"
#include "expression/extract_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression_functors.hpp"
#include "operator/abstract_operator.hpp"
#include "storage/storage_types.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace skyrise::expression_functional;

namespace {

template <typename Functor>
void ResolveBinaryPredicateEvaluator(const PredicateCondition predicate_condition, const Functor functor) {
  /**
   * Instantiate @param functor for each PredicateCondition.
   */
  switch (predicate_condition) {
    case PredicateCondition::kEquals:
      functor(EqualsEvaluator{});
      break;
    case PredicateCondition::kNotEquals:
      functor(NotEqualsEvaluator{});
      break;
    case PredicateCondition::kLessThan:
      functor(LessThanEvaluator{});
      break;
    case PredicateCondition::kLessThanEquals:
      functor(LessThanEqualsEvaluator{});
      break;
    case PredicateCondition::kGreaterThan:
    case PredicateCondition::kGreaterThanEquals:
      Fail("PredicateCondition should have been flipped.");
      break;
    default:
      Fail("PredicateCondition should be handled in different function.");
  }
}

std::shared_ptr<AbstractExpression> RewriteBetweenExpression(const AbstractExpression& expression) {
  /**
   * `a BETWEEN b AND c` ----> `a >= b AND a <= c`
   *
   * This is desirable because three expression data types (from three arguments) generate many type combinations and,
   * thus, lengthen compile time and increase binary size notably.
   */
  const auto* between_expression = dynamic_cast<const BetweenExpression*>(&expression);
  Assert(between_expression, "Expected Between Expression.");

  const std::shared_ptr<BinaryPredicateExpression> lower_expression =
      IsLowerInclusiveBetween(between_expression->GetPredicateCondition())
          ? GreaterThanEquals_(between_expression->Value(), between_expression->LowerBound())
          : GreaterThan_(between_expression->Value(), between_expression->LowerBound());

  const std::shared_ptr<BinaryPredicateExpression> upper_expression =
      IsUpperInclusiveBetween(between_expression->GetPredicateCondition())
          ? LessThanEquals_(between_expression->Value(), between_expression->UpperBound())
          : LessThan_(between_expression->Value(), between_expression->UpperBound());

  return And_(lower_expression, upper_expression);
}

std::shared_ptr<AbstractExpression> RewriteInListExpression(const InExpression& in_expression) {
  /**
   * `a IN (x, y, z)` ----> `a = x OR a = y OR a = z`
   * `a NOT IN (x, y, z)` ----> `a != x AND a != y AND a != z`
   *
   * Out of list_expression->Elements(), pick those expressions whose type can be compared with in_expression.Value(),
   * so we are not getting "Cannot compare Int and String" when querying `5 IN (6, 5, "Hello")`.
   */

  const auto list_expression = std::dynamic_pointer_cast<ListExpression>(in_expression.Set());
  Assert(list_expression, "Expected ListExpression.");

  const bool left_is_string = in_expression.Value()->GetDataType() == DataType::kString;

  const std::vector<std::shared_ptr<AbstractExpression>>& elements = list_expression->Elements();

  std::vector<std::shared_ptr<AbstractExpression>> type_compatible_elements;
  type_compatible_elements.reserve(elements.size());

  for (const auto& element : elements) {
    if ((element->GetDataType() == DataType::kString) == left_is_string) {
      type_compatible_elements.emplace_back(element);
    }
  }

  if (type_compatible_elements.empty()) {
    // `5 IN ()` is FALSE as is `NULL IN ()`.
    return Value_(0);
  }

  std::shared_ptr<AbstractExpression> rewritten_expression;

  if (in_expression.IsNegated()) {
    // `a NOT IN (1,2,3)` ----> `a != 1 AND a != 2 AND a != 3`.
    rewritten_expression = NotEquals_(in_expression.Value(), type_compatible_elements.front());
    for (size_t i = 1; i < type_compatible_elements.size(); ++i) {
      const auto equals_element = NotEquals_(in_expression.Value(), type_compatible_elements[i]);
      rewritten_expression = And_(rewritten_expression, equals_element);
    }
  } else {
    // `a IN (1,2,3)` ----> `a == 1 OR a == 2 OR a == 3`.
    rewritten_expression = Equals_(in_expression.Value(), type_compatible_elements.front());
    for (size_t i = 1; i < type_compatible_elements.size(); ++i) {
      const auto equals_element = Equals_(in_expression.Value(), type_compatible_elements[i]);
      rewritten_expression = Or_(rewritten_expression, equals_element);
    }
  }

  return rewritten_expression;
}

}  // namespace

ExpressionEvaluator::ExpressionEvaluator(const std::shared_ptr<const Table>& table, const ChunkId chunk_id)
    : table_(table), chunk_(table_->GetChunk(chunk_id)), chunk_id_(chunk_id), output_row_count_(chunk_->Size()) {
  segment_materializations_.resize(chunk_->GetColumnCount());
}

std::shared_ptr<BaseValueSegment> ExpressionEvaluator::EvaluateExpressionToSegment(
    const AbstractExpression& expression) {
  std::shared_ptr<BaseValueSegment> segment;
  std::vector<bool> nulls;

  ResolveToExpressionResultView(expression, [&](const auto& view) {
    using ColumnDataType = typename std::decay_t<decltype(view)>::Type;

    if constexpr (std::is_same_v<ColumnDataType, NullValue>) {
      Fail("Cannot create a Segment from a NULL.");
    } else {
      std::vector<ColumnDataType> values(output_row_count_);

      for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(output_row_count_); ++i) {
        values[i] = std::move(view.Value(i));
      }

      if (view.IsNullable()) {
        nulls.resize(output_row_count_);
        for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(output_row_count_); ++i) {
          nulls[i] = view.IsNull(i);
        }
        segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(nulls));
      } else {
        segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
      }
    }
  });

  return segment;
}

RowIdPositionList ExpressionEvaluator::EvaluateExpressionToPositionList(const AbstractExpression& expression) {
  /**
   * Only Expressions returning a Bool can be evaluated to a RowIdPositionList of matches.
   *
   * (Not)In and (Not)Like Expressions are evaluated by generating an ExpressionResult of booleans
   * (EvaluateExpressionToResult<>()), which is then scanned for positive entries.
   */

  RowIdPositionList result_position_list;

  switch (expression.GetExpressionType()) {
    case ExpressionType::kPredicate: {
      // NOLINTNEXTLINE (cppcoreguidelines-pro-type-static-cast-downcast)
      const auto& predicate_expression = static_cast<const AbstractPredicateExpression&>(expression);

      // To reduce the number of template instantiations, we flip > and >= to < and <=.
      bool flip = false;
      auto predicate_condition = predicate_expression.GetPredicateCondition();

      switch (predicate_expression.GetPredicateCondition()) {
        case PredicateCondition::kGreaterThanEquals:
        case PredicateCondition::kGreaterThan:
          flip = true;
          predicate_condition = FlipPredicateCondition(predicate_condition);
          [[fallthrough]];

        case PredicateCondition::kEquals:
        case PredicateCondition::kLessThanEquals:
        case PredicateCondition::kNotEquals:
        case PredicateCondition::kLessThan: {
          const auto& left = *predicate_expression.GetArguments()[flip ? 1 : 0];
          const auto& right = *predicate_expression.GetArguments()[flip ? 0 : 1];

          ResolveToExpressionResults(left, right, [&](const auto& left_result, const auto& right_result) {
            using LeftDataType = typename std::decay_t<decltype(left_result)>::Type;
            using RightDataType = typename std::decay_t<decltype(right_result)>::Type;

            const std::vector<LeftDataType> left;
            const std::vector<RightDataType> right;

            ResolveBinaryPredicateEvaluator(predicate_condition, [&](const auto functor) {
              using ExpressionFunctorType = typename std::decay_t<decltype(functor)>;

              if constexpr (ExpressionFunctorType::template Supports<ExpressionEvaluator::Bool, LeftDataType,
                                                                     RightDataType>::kValue) {
                for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(output_row_count_); ++i) {
                  if (left_result.IsNull(i) || right_result.IsNull(i)) {
                    continue;
                  }

                  auto matches = ExpressionEvaluator::Bool{0};
                  ExpressionFunctorType{}(matches, left_result.Value(i), right_result.Value(i));
                  if (matches != 0) {
                    result_position_list.emplace_back(RowId{chunk_id_, i});
                  }
                }
              } else {
                Fail("Argument types not compatible.");
              }
            });
          });
        } break;

        case PredicateCondition::kBetweenInclusive:
        case PredicateCondition::kBetweenLowerExclusive:
        case PredicateCondition::kBetweenUpperExclusive:
        case PredicateCondition::kBetweenExclusive:
          return EvaluateExpressionToPositionList(*RewriteBetweenExpression(expression));

        case PredicateCondition::kIsNull:
        case PredicateCondition::kIsNotNull: {
          // NOLINTNEXTLINE (cppcoreguidelines-pro-type-static-cast-downcast)
          const auto& is_null_expression = static_cast<const IsNullExpression&>(expression);

          ResolveToExpressionResultView(*is_null_expression.Operand(), [&](const auto& result) {
            if (is_null_expression.GetPredicateCondition() == PredicateCondition::kIsNull) {
              for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(output_row_count_); ++i) {
                if (result.IsNull(i)) {
                  result_position_list.emplace_back(RowId{chunk_id_, i});
                }
              }
            } else {
              // PredicateCondition::IsNotNull.
              for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(output_row_count_); ++i) {
                if (!result.IsNull(i)) {
                  result_position_list.emplace_back(RowId{chunk_id_, i});
                }
              }
            }
          });
        } break;

        case PredicateCondition::kIn:
        case PredicateCondition::kNotIn:
        case PredicateCondition::kLike:
        case PredicateCondition::kNotLike: {
          // Evaluating (Not)In and (Not)Like to RowIdPositionLists uses EvaluateExpressionToResult() and scans the
          // series it returns for matches. This is probably slower than a dedicated EvaluateExpressionToPositionList
          // implementation for these ExpressionTypes could be. However, a) such implementations would require lots of
          // code (as there is little potential for code sharing between the EvaluateExpressionToPositionList and
          // EvaluateExpressionToResult implementations), and b) Like/In are on the slower end anyway.
          const auto result = EvaluateExpressionToResult<ExpressionEvaluator::Bool>(expression);
          result->AsView([&](const auto& result_view) {
            for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(output_row_count_); ++i) {
              if (result_view.Value(i) != 0 && !result_view.IsNull(i)) {
                result_position_list.emplace_back(RowId{chunk_id_, i});
              }
            }
          });
        } break;
      }
    } break;

    case ExpressionType::kLogical: {
      // NOLINTNEXTLINE (cppcoreguidelines-pro-type-static-cast-downcast)
      const auto& logical_expression = static_cast<const LogicalExpression&>(expression);

      const auto left_pos_list = EvaluateExpressionToPositionList(*logical_expression.GetArguments()[0]);
      const auto right_pos_list = EvaluateExpressionToPositionList(*logical_expression.GetArguments()[1]);

      switch (logical_expression.GetLogicalOperator()) {
        case LogicalOperator::kAnd:
          std::set_intersection(left_pos_list.begin(), left_pos_list.end(), right_pos_list.begin(),
                                right_pos_list.end(), std::back_inserter(result_position_list));
          break;

        case LogicalOperator::kOr:
          std::set_union(left_pos_list.begin(), left_pos_list.end(), right_pos_list.begin(), right_pos_list.end(),
                         std::back_inserter(result_position_list));
          break;
      }
    } break;

    case ExpressionType::kValue: {
      // NOLINTNEXTLINE (cppcoreguidelines-pro-type-static-cast-downcast)
      const auto& value_expression = static_cast<const ValueExpression&>(expression);
      Assert(std::holds_alternative<ExpressionEvaluator::Bool>(value_expression.GetValue()),
             "Cannot evaluate non-boolean literal to PositionList.");
      // `TRUE` literal returns the entire chunk, while `FALSE` literal returns an empty RowIdPositionList.
      if (std::get<ExpressionEvaluator::Bool>(value_expression.GetValue()) != 0) {
        result_position_list.resize(output_row_count_);
        for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(output_row_count_); ++i) {
          result_position_list[i] = {chunk_id_, i};
        }
      }
    } break;

    default:
      Fail("Expression type cannot be evaluated to PositionList.");
  }

  return result_position_list;
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateExpressionToResult(
    const AbstractExpression& expression) {
  // TODO(tobodner): Investigate whether caching works with our serialization/deserialization logic.
  const auto shared_expression = expression.shared_from_this();
  const auto cached_expression_result_iterator = cached_expression_results_.find(shared_expression);

  if (cached_expression_result_iterator != cached_expression_results_.cend()) {
    return std::static_pointer_cast<ExpressionResult<Result>>(cached_expression_result_iterator->second);
  }

  std::shared_ptr<ExpressionResult<Result>> result;

  switch (expression.GetExpressionType()) {
    case ExpressionType::kArithmetic:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      result = EvaluateArithmeticExpression<Result>(static_cast<const ArithmeticExpression&>(expression));
      break;
    case ExpressionType::kCase:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      result = EvaluateCaseExpression<Result>(static_cast<const CaseExpression&>(expression));
      break;
    case ExpressionType::kCast:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      result = EvaluateCastExpression<Result>(static_cast<const CastExpression&>(expression));
      break;

    case ExpressionType::kLogical:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      result = EvaluateLogicalExpression<Result>(static_cast<const LogicalExpression&>(expression));
      break;

    case ExpressionType::kPredicate:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      result = EvaluatePredicateExpression<Result>(static_cast<const AbstractPredicateExpression&>(expression));
      break;

    case ExpressionType::kPqpColumn:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      result = EvaluateColumnExpression<Result>(*static_cast<const PqpColumnExpression*>(&expression));
      break;

    case ExpressionType::kValue:
      result = EvaluateValueOrCorrelatedParameterExpression<Result>(expression);
      break;

    case ExpressionType::kExtract:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      result = EvaluateExtractExpression<Result>(static_cast<const ExtractExpression&>(expression));
      break;

    case ExpressionType::kUnaryMinus:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      result = EvaluateUnaryMinusExpression<Result>(static_cast<const UnaryMinusExpression&>(expression));
      break;

    case ExpressionType::kAggregate:
      Fail("ExpressionEvaluator does not support Aggregates. Use the Aggregate Operator to compute them.");

    case ExpressionType::kList:
      Fail("Cannot evaluate a ListExpression. Lists should only appear as the right operand of an InExpression.");
  }

  // Store the result in the cache.
  cached_expression_results_.insert(cached_expression_result_iterator, {shared_expression, result});

  return std::static_pointer_cast<ExpressionResult<Result>>(result);
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateArithmeticExpression(
    const ArithmeticExpression& expression) {
  const AbstractExpression& left = *expression.LeftOperand();
  const AbstractExpression& right = *expression.RightOperand();

  switch (expression.GetArithmeticOperator()) {
    case ArithmeticOperator::kAddition:
      return EvaluateBinaryWithDefaultNullLogic<Result, AdditionEvaluator>(left, right);
    case ArithmeticOperator::kSubtraction:
      return EvaluateBinaryWithDefaultNullLogic<Result, SubtractionEvaluator>(left, right);
    case ArithmeticOperator::kMultiplication:
      return EvaluateBinaryWithDefaultNullLogic<Result, MultiplicationEvaluator>(left, right);
    case ArithmeticOperator::kDivision:
      return EvaluateBinaryWithFunctorBasedNullLogic<Result, DivisionEvaluator>(left, right);
    case ArithmeticOperator::kModulo:
      return EvaluateBinaryWithFunctorBasedNullLogic<Result, ModuloEvaluator>(left, right);
  }
  Fail("Invalid enum value.");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::EvaluateBinaryPredicateExpression<ExpressionEvaluator::Bool>(
    const BinaryPredicateExpression& expression) {
  std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>> result;

  // To reduce the number of template instantiations, we flip > and >= to < and <=.
  auto predicate_condition = expression.GetPredicateCondition();
  const bool flip = predicate_condition == PredicateCondition::kGreaterThan ||
                    predicate_condition == PredicateCondition::kGreaterThanEquals;
  if (flip) {
    predicate_condition = FlipPredicateCondition(predicate_condition);
  }
  const AbstractExpression& left = flip ? *expression.RightOperand() : *expression.LeftOperand();
  const AbstractExpression& right = flip ? *expression.LeftOperand() : *expression.RightOperand();

  ResolveBinaryPredicateEvaluator(predicate_condition, [&](const auto evaluator_t) {
    using Evaluator = typename std::decay_t<decltype(evaluator_t)>;
    result = EvaluateBinaryWithDefaultNullLogic<ExpressionEvaluator::Bool, Evaluator>(left, right);
  });

  return result;
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateBinaryPredicateExpression(
    const BinaryPredicateExpression& /*expression*/) {
  Fail("Can only evaluate predicates to bool.");
}

template <typename Result, typename Functor>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateBinaryWithDefaultNullLogic(
    const AbstractExpression& left_expression, const AbstractExpression& right_expression) {
  std::vector<Result> values;
  std::vector<bool> nulls;

  ResolveToExpressionResults(left_expression, right_expression, [&](const auto& left_result, const auto& right_result) {
    using LeftDataType = typename std::decay_t<decltype(left_result)>::Type;
    using RightDataType = typename std::decay_t<decltype(right_result)>::Type;

    if constexpr (Functor::template Supports<Result, LeftDataType, RightDataType>::kValue) {
      const ChunkOffset result_size = ResultSize(left_result.Size(), right_result.Size());
      values.resize(result_size);

      nulls = EvaluateDefaultNullLogic(left_result.GetNulls(), right_result.GetNulls());

      if (left_result.IsLiteral() == right_result.IsLiteral()) {
        for (ChunkOffset i = 0; i < result_size; ++i) {
          Functor{}(values[i], left_result.GetValues()[i], right_result.GetValues()[i]);
        }
      } else if (right_result.IsLiteral()) {
        for (ChunkOffset i = 0; i < result_size; ++i) {
          Functor{}(values[i], left_result.GetValues()[i], right_result.GetValues().front());
        }
      } else {
        for (ChunkOffset i = 0; i < result_size; ++i) {
          Functor{}(values[i], left_result.GetValues().front(), right_result.GetValues()[i]);
        }
      }
    } else {
      Fail("BinaryOperation not supported on the requested DataTypes.");
    }
  });

  return std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));
}

template <typename Result, typename Functor>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateBinaryWithFunctorBasedNullLogic(
    const AbstractExpression& left_expression, const AbstractExpression& right_expression) {
  auto result = std::make_shared<ExpressionResult<Result>>();

  ResolveToExpressionResultViews(left_expression, right_expression, [&](const auto& left, const auto& right) {
    using LeftDataType = typename std::decay_t<decltype(left)>::Type;
    using RightDataType = typename std::decay_t<decltype(right)>::Type;

    if constexpr (Functor::template Supports<Result, LeftDataType, RightDataType>::kValue) {
      const ChunkOffset result_row_count = ResultSize(left.Size(), right.Size());

      std::vector<bool> nulls(result_row_count);
      std::vector<Result> values(result_row_count);

      for (ChunkOffset i = 0; i < result_row_count; ++i) {
        bool null = false;
        Functor{}(values[i], null, left.Value(i), left.IsNull(i), right.Value(i), right.IsNull(i));
        nulls[i] = null;
      }

      result = std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));

    } else {
      Fail("BinaryOperation not supported on the requested DataTypes.");
    }
  });

  return result;
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateCaseExpression(
    const CaseExpression& case_expression) {
  const auto when = EvaluateExpressionToResult<ExpressionEvaluator::Bool>(*case_expression.When());

  std::vector<Result> values;
  std::vector<bool> nulls;

  ResolveToExpressionResults(
      *case_expression.Then(), *case_expression.Otherwise(), [&](const auto& then_result, const auto& else_result) {
        using ThenResultType = typename std::decay_t<decltype(then_result)>::Type;
        using ElseResultType = typename std::decay_t<decltype(else_result)>::Type;

        if constexpr (CaseEvaluator::kSupportsV<Result, ThenResultType, ElseResultType>) {
          const auto result_size = ResultSize(when->Size(), then_result.Size(), else_result.Size());
          values.resize(result_size);
          nulls.resize(result_size);

          for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(result_size); ++i) {
            if (when->Value(i) && !when->IsNull(i)) {
              values[i] = ToValue<Result>(then_result.Value(i));
              nulls[i] = then_result.IsNull(i);
            } else {
              values[i] = ToValue<Result>(else_result.Value(i));
              nulls[i] = else_result.IsNull(i);
            }
          }
        } else {
          Fail("Illegal operands for CaseExpression.");
        }
      });

  return std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateCastExpression(
    const CastExpression& cast_expression) {
  /**
   * Implements SQL's CAST with the following semantics.
   *
   * Float/Double ----> Int/Long: Value gets floor()ed.
   * String ----> Int/Long/Float/Double: Conversion is attempted, on error zero is returned (e.g., `" 5hallo" AS INT)`
   * ----> `5`). NULL -> Any: A nulled value of the requested type is returned.
   */

  std::vector<Result> values;
  std::vector<bool> nulls;

  ResolveToExpressionResult(*cast_expression.Argument(), [&](const auto& argument_result) {
    using ArgumentDataType = typename std::decay_t<decltype(argument_result)>::Type;

    const auto result_size = ResultSize(argument_result.Size());

    values.resize(result_size);

    for (ChunkOffset i = 0; i < result_size; ++i) {
      const auto& argument_value = argument_result.Value(i);

      // NOLINTNEXTLINE(bugprone-branch-clone)
      if constexpr (std::is_same_v<Result, NullValue> || std::is_same_v<ArgumentDataType, NullValue>) {
        // `<Something> to NULL` cast. Do nothing; handled by the `nulls` vector.
      } else if constexpr (std::is_same_v<Result, std::string>) {
        // `<Something> to String` cast. Should never fail; therefore, boost::lexical_cast (which throws on error) is
        // fine.
        values[i] = boost::lexical_cast<Result>(argument_value);
      } else {
        if constexpr (std::is_same_v<ArgumentDataType, std::string>) {
          // `String to Numeric` cast. An illegal conversion (e.g., `CAST("Hello" AS INT)`) yields zero. Does not use
          // `boost::lexical_cast()` as that would throw on error.
          if (!boost::conversion::try_lexical_convert(argument_value, values[i])) {
            values[i] = 0;
          }
        } else {
          // `Numeric to Numeric` cast. Use static_cast<> as boost::conversion::try_lexical_convert() would fail for
          // `CAST(5.5 AS INT)`.
          values[i] = static_cast<Result>(argument_value);
        }
      }
    }

    nulls = argument_result.GetNulls();
  });

  return std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateColumnExpression(
    const PqpColumnExpression& column_expression) {
  Assert(chunk_, "Cannot access columns in this Expression as it does not operate on a Table/Chunk.");

  const auto& segment = *chunk_->GetSegment(column_expression.GetColumnId());
  Assert(segment.GetDataType() == DataTypeFromType<Result>(), "Cannot evaluate segment to different type.");

  MaterializeSegmentIfNotYetMaterialized(column_expression.GetColumnId());
  return std::static_pointer_cast<ExpressionResult<Result>>(segment_materializations_[column_expression.GetColumnId()]);
}

template <>
std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::EvaluateExtractExpression<std::string>(
    const ExtractExpression& extract_expression) {
  const std::shared_ptr<ExpressionResult<std::string>> from_result =
      EvaluateExpressionToResult<std::string>(*extract_expression.From());

  switch (extract_expression.GetDatetimeComponent()) {
    case DatetimeComponent::kYear:
      return EvaluateExtractSubstr<0, 4>(*from_result);
    case DatetimeComponent::kMonth:
      return EvaluateExtractSubstr<5, 2>(*from_result);
    case DatetimeComponent::kDay:
      return EvaluateExtractSubstr<8, 2>(*from_result);
    case DatetimeComponent::kHour:
    case DatetimeComponent::kMinute:
    case DatetimeComponent::kSecond:
      Fail("Hour, Minute, and Second not available in String Datetimes.");
  }
  Fail("Invalid enum value.");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateExtractExpression(
    const ExtractExpression& /*extract_expression*/) {
  Fail("Only Strings (YYYY-MM-DD) supported for Dates.");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::EvaluateInExpression<ExpressionEvaluator::Bool>(const InExpression& in_expression) {
  const AbstractExpression& left_expression = *in_expression.Value();
  const AbstractExpression& right_expression = *in_expression.Set();

  std::vector<ExpressionEvaluator::Bool> result_values;
  std::vector<bool> result_nulls;

  if (right_expression.GetExpressionType() == ExpressionType::kList) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    const auto& list_expression = static_cast<const ListExpression&>(right_expression);

    if (list_expression.Elements().empty()) {
      // `x IN ()` is false while `x NOT IN ()` is true, even if that is not supported by SQL.
      return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(
          // NOLINTNEXTLINE(readability-implicit-bool-conversion)
          std::vector<ExpressionEvaluator::Bool>{in_expression.IsNegated()});
    }

    if (left_expression.GetDataType() == DataType::kNull) {
      // `NULL [NOT] IN ...` is `NULL`.
      return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::vector<ExpressionEvaluator::Bool>{0},
                                                                           std::vector<bool>{true});
    }

    const bool left_is_string = left_expression.GetDataType() == DataType::kString;
    std::vector<std::shared_ptr<AbstractExpression>> type_compatible_elements;
    bool all_elements_are_values_of_left_type = true;
    ResolveDataType(left_expression.GetDataType(), [&](const auto left_data_type_t) {
      using LeftDataType = typename std::decay<decltype(left_data_type_t)>::type;

      for (const auto& element : list_expression.Elements()) {
        if ((element->GetDataType() == DataType::kString) == left_is_string) {
          type_compatible_elements.emplace_back(element);
        }

        if (element->GetExpressionType() != ExpressionType::kValue) {
          all_elements_are_values_of_left_type = false;
        } else {
          const auto value_expression = std::static_pointer_cast<ValueExpression>(element);
          if (!std::holds_alternative<LeftDataType>(value_expression->GetValue())) {
            all_elements_are_values_of_left_type = false;
          }
        }
      }
    });

    if (type_compatible_elements.empty()) {
      // `x IN ()` is false while `x NOT IN ()` is true, even if that is not supported by SQL.
      return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(
          // NOLINTNEXTLINE(readability-implicit-bool-conversion)
          std::vector<ExpressionEvaluator::Bool>{in_expression.IsNegated()});
    }

    // If all elements of the list are simple values (e.g., `IN (1, 2, 3)`), iterate over the column and directly
    // compare the left value with the values in the list. If we cannot store the values in a vector (e.g., because they
    // are of non-literals or of different types), we translate the `IN` clause to a series of `OR`s. `a IN (x, y, z)`
    // ----> `a = x OR a = y OR a = z`. The first path is faster, while the second one is more flexible.
    if (all_elements_are_values_of_left_type) {
      ResolveToExpressionResultView(left_expression, [&](const auto& left_view) {
        using LeftDataType = typename std::decay_t<decltype(left_view)>::Type;

        // Above, we have ruled out `NULL` on the left side, but the compiler does not know this yet.
        if constexpr (!std::is_same_v<LeftDataType, NullValue>) {
          std::vector<LeftDataType> right_values(type_compatible_elements.size());
          size_t i = 0;
          for (const auto& expression : type_compatible_elements) {
            const auto& value_expression = std::static_pointer_cast<ValueExpression>(expression);
            right_values[i] = std::get<LeftDataType>(value_expression->GetValue());
            ++i;
          }

          result_values.resize(left_view.Size(), in_expression.IsNegated());
          if (left_view.IsNullable()) {
            result_nulls.resize(left_view.Size());
          }

          for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(left_view.Size()); ++i) {
            if (left_view.IsNullable() && left_view.IsNull(i)) {
              result_nulls[i] = true;
              continue;
            }
            // We could sort right_values and perform a binary search. However, a linear search is better suited for
            // small vectors. For bigger `IN` lists, the InExpressionRewriteRule will switch to a hash join.
            auto iterator = std::find(right_values.cbegin(), right_values.cend(), left_view.Value(i));
            if (iterator != right_values.cend() && *iterator == left_view.Value(i)) {
              result_values[i] = !in_expression.IsNegated();
            }
          }
        } else {
          Fail("Should have ruled out NullValues on the left side of IN by now.");
        }
      });

      return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::move(result_values),
                                                                           std::move(result_nulls));
    }

    // List with diverse types; falling back to rewrite of expression.
    return EvaluateExpressionToResult<ExpressionEvaluator::Bool>(*RewriteInListExpression(in_expression));
  } else {
    /**
     * `<expression> IN <anything_but_list_or_subquery>` is not legal SQL, but on expression level we have to support
     * it, since `<anything_but_list_or_subquery>` might be a column holding the result of a subquery. To accomplish
     * this, we simply rewrite the expression to `<expression> IN LIST(<anything_but_list_or_subquery>)`.
     */

    return EvaluateInExpression<ExpressionEvaluator::Bool>(*std::make_shared<InExpression>(
        in_expression.GetPredicateCondition(), in_expression.Value(), List_(in_expression.Set())));
  }

  return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::move(result_values),
                                                                       std::move(result_nulls));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateInExpression(
    const InExpression& /*in_expression*/) {
  Fail("InExpression supports only bool as result.");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::EvaluateIsNullExpression<ExpressionEvaluator::Bool>(const IsNullExpression& expression) {
  std::vector<ExpressionEvaluator::Bool> result_values;

  ResolveToExpressionResultView(*expression.Operand(), [&](const auto& view) {
    const size_t view_size = view.Size();
    result_values.resize(view_size);

    if (expression.GetPredicateCondition() == PredicateCondition::kIsNull) {
      for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(view_size); ++i) {
        result_values[i] = view.IsNull(i);
      }
    } else {
      // PredicateCondition::kIsNotNull.
      for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(view_size); ++i) {
        result_values[i] = !view.IsNull(i);
      }
    }
  });

  return std::make_shared<ExpressionResult<ExpressionEvaluator::Bool>>(std::move(result_values));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateIsNullExpression(
    const IsNullExpression& /*expression*/) {
  Fail("Can only evaluate predicates to bool.");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::EvaluateLogicalExpression<ExpressionEvaluator::Bool>(const LogicalExpression& expression) {
  const AbstractExpression& left = *expression.LeftOperand();
  const AbstractExpression& right = *expression.RightOperand();

  switch (expression.GetLogicalOperator()) {
    case LogicalOperator::kOr:
      return EvaluateBinaryWithFunctorBasedNullLogic<ExpressionEvaluator::Bool, TernaryOrEvaluator>(left, right);
    case LogicalOperator::kAnd:
      return EvaluateBinaryWithFunctorBasedNullLogic<ExpressionEvaluator::Bool, TernaryAndEvaluator>(left, right);
  }

  Fail("Invalid enum value.");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateLogicalExpression(
    const LogicalExpression& /*expression*/) {
  Fail("LogicalExpression can only output bool.");
}

template <>
std::shared_ptr<ExpressionResult<ExpressionEvaluator::Bool>>
ExpressionEvaluator::EvaluatePredicateExpression<ExpressionEvaluator::Bool>(
    const AbstractPredicateExpression& predicate_expression) {
  /**
   * Evaluates predicates, but typical predicates in the `WHERE` clause of an SQL query will not take this path and go
   * through a dedicates scan operator (e.g., TableScan).
   */

  switch (predicate_expression.GetPredicateCondition()) {
    case PredicateCondition::kEquals:
    case PredicateCondition::kLessThanEquals:
    case PredicateCondition::kGreaterThanEquals:
    case PredicateCondition::kGreaterThan:
    case PredicateCondition::kNotEquals:
    case PredicateCondition::kLessThan:
      return EvaluateBinaryPredicateExpression<ExpressionEvaluator::Bool>(
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
          static_cast<const BinaryPredicateExpression&>(predicate_expression));

    case PredicateCondition::kBetweenInclusive:
    case PredicateCondition::kBetweenLowerExclusive:
    case PredicateCondition::kBetweenUpperExclusive:
    case PredicateCondition::kBetweenExclusive:
      return EvaluateExpressionToResult<ExpressionEvaluator::Bool>(*RewriteBetweenExpression(predicate_expression));

    case PredicateCondition::kIn:
    case PredicateCondition::kNotIn:
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
      return EvaluateInExpression<ExpressionEvaluator::Bool>(static_cast<const InExpression&>(predicate_expression));

    case PredicateCondition::kLike:
    case PredicateCondition::kNotLike:
      Fail("Predicate kLike and kNotLike not supported yet.");

    case PredicateCondition::kIsNull:
    case PredicateCondition::kIsNotNull:
      return EvaluateIsNullExpression<ExpressionEvaluator::Bool>(
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
          static_cast<const IsNullExpression&>(predicate_expression));
  }
  Fail("Invalid enum value.");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluatePredicateExpression(
    const AbstractPredicateExpression& /*predicate_expression*/) {
  Fail("Can only evaluate predicates to bool.");
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateUnaryMinusExpression(
    const UnaryMinusExpression& unary_minus_expression) {
  std::vector<Result> values;
  std::vector<bool> nulls;

  ResolveToExpressionResult(*unary_minus_expression.Argument(), [&](const auto& argument_result) {
    using ArgumentType = typename std::decay_t<decltype(argument_result)>::Type;

    if constexpr (!std::is_same_v<ArgumentType, std::string> && std::is_same_v<Result, ArgumentType>) {
      values.resize(argument_result.Size());
      for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(argument_result.Size()); ++i) {
        values[i] = -argument_result.GetValues()[i];
      }
      nulls = argument_result.GetNulls();
    } else {
      Fail("Cannot negate a strings; cannot negate an argument to a different type.");
    }
  });

  return std::make_shared<ExpressionResult<Result>>(std::move(values), std::move(nulls));
}

template <typename Result>
std::shared_ptr<ExpressionResult<Result>> ExpressionEvaluator::EvaluateValueOrCorrelatedParameterExpression(
    const AbstractExpression& expression) {
  AllTypeVariant value;

  if (expression.GetExpressionType() == ExpressionType::kValue) {
    // NOLINTNEXTLINE (cppcoreguidelines-pro-type-static-cast-downcast)
    const auto& value_expression = static_cast<const ValueExpression&>(expression);
    value = value_expression.GetValue();
  }
  if (VariantIsNull(value)) {
    // NullValue can be evaluated to any type; it is then a null value of that type. That makes it easier to implement
    // expressions where a certain data type is expected, but a Null literal is given. Think of `CASE NULL THEN ... ELSE
    // ...`; the `NULL` will be evaluated to be a bool.
    const std::vector<bool> nulls = {true};
    return std::make_shared<ExpressionResult<Result>>(std::vector<Result>{{Result{}}}, nulls);
  } else {
    Assert(std::holds_alternative<Result>(value), "Cannot evaluate ValueExpression to requested type Result.");
    return std::make_shared<ExpressionResult<Result>>(std::vector<Result>{{std::get<Result>(value)}});
  }
}

template <size_t Offset, size_t Count>
std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::EvaluateExtractSubstr(
    const ExpressionResult<std::string>& from_result) {
  const std::shared_ptr<ExpressionResult<std::string>> result;

  std::vector<std::string> values(from_result.Size());

  from_result.AsView([&](const auto& from_view) {
    for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(from_view.Size()); ++i) {
      if (!from_view.IsNull(i)) {
        DebugAssert(from_view.Value(i).size() == 10,
                    "Invalid DatetimeString '" + std::string(from_view.Value(i)) + "'.");
        values[i] = from_view.Value(i).substr(Offset, Count);
      }
    }
  });

  return std::make_shared<ExpressionResult<std::string>>(std::move(values), from_result.GetNulls());
}

template <typename Functor>
void ExpressionEvaluator::ResolveToExpressionResultView(const AbstractExpression& expression, const Functor& functor) {
  ResolveToExpressionResult(expression,
                            [&](const auto& result) { result.AsView([&](const auto& view) { functor(view); }); });
}

template <typename Functor>
void ExpressionEvaluator::ResolveToExpressionResultViews(const AbstractExpression& left_expression,
                                                         const AbstractExpression& right_expression,
                                                         const Functor& functor) {
  ResolveToExpressionResults(left_expression, right_expression, [&](const auto& left_result, const auto& right_result) {
    left_result.AsView([&](const auto& left_view) {
      right_result.AsView([&](const auto& right_view) { functor(left_view, right_view); });
    });
  });
}

template <typename Functor>
void ExpressionEvaluator::ResolveToExpressionResult(const AbstractExpression& expression, const Functor& functor) {
  Assert(expression.GetExpressionType() != ExpressionType::kList, "Cannot resolve ListExpression to ExpressionResult.");

  if (expression.GetDataType() == DataType::kNull) {
    // ResolveDataType() does not support DataType::kNull, so we have to handle it explicitly.
    const ExpressionResult<NullValue> null_value_result({NullValue{}}, {true});

    functor(null_value_result);

  } else {
    ResolveDataType(expression.GetDataType(), [&](const auto data_type_t) {
      using ExpressionDataType = typename std::decay<decltype(data_type_t)>::type;

      const auto expression_result = EvaluateExpressionToResult<ExpressionDataType>(expression);
      functor(*expression_result);
    });
  }
}

template <typename Functor>
void ExpressionEvaluator::ResolveToExpressionResults(const AbstractExpression& left_expression,
                                                     const AbstractExpression& right_expression,
                                                     const Functor& functor) {
  ResolveToExpressionResult(left_expression, [&](const auto& left_result) {
    ResolveToExpressionResult(right_expression, [&](const auto& right_result) { functor(left_result, right_result); });
  });
}

template <typename... RowCounts>
ChunkOffset ExpressionEvaluator::ResultSize(const RowCounts... row_counts) {
  /**
   * If any operand is empty (i.e., if it is an empty segment) the result of the expression has no rows.
   *
   * ResultSize() covers the following scenarios.
   *   - Column-involving expression evaluation on an empty Chunk should yield zero rows. `a + 5` should be empty on an
   *     empty Chunk.
   *   - If the Chunk is not empty, Literal-and-Column-involving expression evaluation should yield one result per row.
   *     `a + 5` should yield one value for each element in `a`.
   *   - Non-column involving expressions should yield one result value, regardless of whether there is a (potentially)
   *     non-empty Chunk involved or not. `5 + 3` always yields one result element, 8.
   */

  if (((row_counts == 0) || ...)) {
    return 0;
  }

  return static_cast<ChunkOffset>(std::max({row_counts...}));
}

std::vector<bool> ExpressionEvaluator::EvaluateDefaultNullLogic(const std::vector<bool>& left,
                                                                const std::vector<bool>& right) {
  if (left.size() == right.size()) {
    std::vector<bool> nulls(left.size());
    std::transform(left.begin(), left.end(), right.begin(), nulls.begin(), [](auto l, auto r) { return l || r; });
    return nulls;
  } else if (left.size() > right.size()) {
    DebugAssert(right.size() <= 1,
                "Operand should have either the same row count as the other, 1 row (to represent a literal), or no "
                "rows (to represent a non-nullable operand).");
    if (!right.empty() && right.front()) {
      return {true};
    } else {
      return left;
    }
  } else {
    DebugAssert(left.size() <= 1,
                "Operand should have either the same row count as the other, 1 row (to represent a literal), or no "
                "rows (to represent a non-nullable operand).");
    if (!left.empty() && left.front()) {
      return {true};
    } else {
      return right;
    }
  }
}

void ExpressionEvaluator::MaterializeSegmentIfNotYetMaterialized(const ColumnId column_id) {
  Assert(chunk_, "Cannot access columns in this Expression as it does not operate on a Table/Chunk.");

  if (segment_materializations_[column_id]) {
    return;
  }

  const auto& segment = *chunk_->GetSegment(column_id);

  ResolveDataType(segment.GetDataType(), [&](const auto column_data_type_t) {
    using ColumnDataType = std::decay_t<decltype(column_data_type_t)>;

    std::vector<ColumnDataType> values;
    std::vector<bool> nulls;

    if (const auto* value_segment = dynamic_cast<const ValueSegment<ColumnDataType>*>(&segment)) {
      values = value_segment->Values();
      if (table_->ColumnIsNullable(column_id)) {
        nulls = value_segment->NullValues();
      }
    }

    if (table_->ColumnIsNullable(column_id)) {
      segment_materializations_[column_id] =
          std::make_shared<ExpressionResult<ColumnDataType>>(std::move(values), std::move(nulls));
    } else {
      segment_materializations_[column_id] = std::make_shared<ExpressionResult<ColumnDataType>>(std::move(values));
    }
  });
}

std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::EvaluateSubstring(
    const std::vector<std::shared_ptr<AbstractExpression>>& arguments) {
  DebugAssert(arguments.size() == 3, "SUBSTR expects three arguments.");

  const auto strings = EvaluateExpressionToResult<std::string>(*arguments[0]);
  const auto starts = EvaluateExpressionToResult<int32_t>(*arguments[1]);
  const auto lengths = EvaluateExpressionToResult<int32_t>(*arguments[2]);

  const ChunkOffset row_count = ResultSize(strings->Size(), starts->Size(), lengths->Size());

  std::vector<std::string> result_values(row_count);
  std::vector<bool> result_nulls(row_count);

  for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(row_count); ++i) {
    result_nulls[i] = strings->IsNull(i) || starts->IsNull(i) || lengths->IsNull(i);

    const auto& string = strings->Value(i);
    DebugAssert(string.size() < size_t{std::numeric_limits<int32_t>::max()},
                "String is too long to be handled by SUBSTR. Switch to int64_t in the SUBSTR implementation.");

    const auto signed_string_size = static_cast<int32_t>(string.size());

    auto length = lengths->Value(i);
    if (length <= 0) {
      continue;
    }

    auto start = starts->Value(i);

    /**
     * `SUBSTR` follows the below semantics for negative indices.
     *
     * START -8 -7 -6 -5 -4 -3 -2 -1 || 0  || 1 2 3 4 5 6  7  8
     * CHAR  // // // H  e  l  l  o  || // || H e l l o // // //
     *
     * `SUBSTR('HELLO', 0, 2)` ----> 'H'
     * `SUBSTR('HELLO', -1, 2)` ----> 'O'
     * `SUBSTR('HELLO', -8, 1)` ----> ''
     * `SUBSTR('HELLO', -8, 5)` ----> 'HE'
     */
    int32_t end = 0;
    if (start < 0) {
      start += signed_string_size;
    } else {
      if (start == 0) {
        length -= 1;
      } else {
        start -= 1;
      }
    }

    end = start + length;
    start = std::max(0, start);
    end = std::min(end, signed_string_size);
    length = end - start;

    // Invalid/out of range arguments, e.g., `SUBSTR("HELLO", 4000, -2)`, lead to an empty string.
    if (!string.empty() && start >= 0 && start < signed_string_size && length > 0) {
      length = std::min<int32_t>(signed_string_size - start, length);
      result_values[i] = string.substr(static_cast<size_t>(start), static_cast<size_t>(length));
    }
  }

  return std::make_shared<ExpressionResult<std::string>>(result_values, result_nulls);
}

std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::EvaluateConcatenate(
    const std::vector<std::shared_ptr<AbstractExpression>>& arguments) {
  /**
   * `CONCAT`, e.g., returning `NULL` once any argument is `NULL`.
   */

  std::vector<std::shared_ptr<ExpressionResult<std::string>>> argument_results;
  argument_results.reserve(arguments.size());

  auto result_is_nullable = false;

  // Compute the arguments.
  for (const auto& argument : arguments) {
    // `CONCAT` with a `NULL` literal argument yields `NULL`.
    if (argument->GetDataType() == DataType::kNull) {
      const ExpressionResult<std::string> null_value_result({std::string()}, {true});
      return std::make_shared<ExpressionResult<std::string>>(null_value_result);
    }

    const auto argument_result = EvaluateExpressionToResult<std::string>(*argument);
    argument_results.emplace_back(argument_result);

    result_is_nullable |= argument_result->IsNullable();
  }

  // Compute the number of output rows.
  size_t result_size = argument_results.empty() ? 0 : argument_results.front()->Size();
  for (size_t i = 1; i < argument_results.size(); ++i) {
    result_size = ResultSize(result_size, argument_results[i]->Size());
  }

  // Concatenate the values.
  std::vector<std::string> result_values(result_size);
  for (const auto& argument_result : argument_results) {
    argument_result->AsView([&](const auto& argument_view) {
      for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(result_size); ++i) {
        // The actual CONCAT
        result_values[i] += argument_view.Value(i);
      }
    });
  }

  // Optionally, concatenate the nulls (i.e., one argument is `NULL` yields `NULL`) and return.
  std::vector<bool> result_nulls;
  if (result_is_nullable) {
    result_nulls.resize(result_size, false);
    for (const auto& argument_result : argument_results) {
      argument_result->AsView([&](const auto& argument_view) {
        for (ChunkOffset i = 0; i < static_cast<ChunkOffset>(result_size); ++i) {
          if (argument_view.IsNull(i)) {
            result_nulls[i] = true;
          }
        }
      });
    }
  }

  return std::make_shared<ExpressionResult<std::string>>(std::move(result_values), std::move(result_nulls));
}

template std::shared_ptr<ExpressionResult<int32_t>> ExpressionEvaluator::EvaluateExpressionToResult<int32_t>(
    const AbstractExpression& expression);
template std::shared_ptr<ExpressionResult<int64_t>> ExpressionEvaluator::EvaluateExpressionToResult<int64_t>(
    const AbstractExpression& expression);
template std::shared_ptr<ExpressionResult<float>> ExpressionEvaluator::EvaluateExpressionToResult<float>(
    const AbstractExpression& expression);
template std::shared_ptr<ExpressionResult<double>> ExpressionEvaluator::EvaluateExpressionToResult<double>(
    const AbstractExpression& expression);
template std::shared_ptr<ExpressionResult<std::string>> ExpressionEvaluator::EvaluateExpressionToResult<std::string>(
    const AbstractExpression& expression);

}  // namespace skyrise
