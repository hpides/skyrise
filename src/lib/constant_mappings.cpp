/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "constant_mappings.hpp"

namespace {

/**
 * Boost.Bimap does not support initializer_lists.
 * Instead, we use this helper function to have an initializer_list-friendly interface.
 */
template <typename L, typename R>
boost::bimap<L, R> MakeBimap(std::initializer_list<typename boost::bimap<L, R>::value_type> list) {
  return boost::bimap<L, R>(list.begin(), list.end());
}

}  //  namespace

namespace skyrise {

const boost::bimap<AggregateFunction, std::string> kAggregateFunctionToString =
    MakeBimap<AggregateFunction, std::string>({{AggregateFunction::kAny, "ANY"},
                                               {AggregateFunction::kAvg, "AVG"},
                                               {AggregateFunction::kCount, "COUNT"},
                                               {AggregateFunction::kCountDistinct, "COUNT DISTINCT"},
                                               {AggregateFunction::kMax, "MAX"},
                                               {AggregateFunction::kMin, "MIN"},
                                               {AggregateFunction::kStandardDeviationSample, "STDDEV_SAMP"},
                                               {AggregateFunction::kStringAgg, "STRING_AGG"},
                                               {AggregateFunction::kSum, "SUM"}});

const boost::bimap<ExchangeMode, std::string> kExchangeModeToString = MakeBimap<ExchangeMode, std::string>({
    {ExchangeMode::kFullMerge, "Full Merge"},
    {ExchangeMode::kPartialMerge, "Partial Merge"},
});

const boost::bimap<DataType, std::string> kDataTypeToString = MakeBimap<DataType, std::string>({
    {DataType::kDouble, "double"},
    {DataType::kFloat, "float"},
    {DataType::kInt, "int"},
    {DataType::kLong, "long"},
    {DataType::kString, "string"},
});

const boost::bimap<JoinMode, std::string> kJoinModeToString = MakeBimap<JoinMode, std::string>({
    {JoinMode::kAntiNullAsFalse, "AntiNullAsFalse"},
    {JoinMode::kAntiNullAsTrue, "AntiNullAsTrue"},
    {JoinMode::kCross, "Cross"},
    {JoinMode::kFullOuter, "Full Outer"},
    {JoinMode::kInner, "Inner"},
    {JoinMode::kLeftOuter, "Left Outer"},
    {JoinMode::kRightOuter, "Right Outer"},
    {JoinMode::kSemi, "Semi"},
});

const boost::bimap<PredicateCondition, std::string> kPredicateConditionToString =
    MakeBimap<PredicateCondition, std::string>({
        {PredicateCondition::kBetweenExclusive, "BETWEEN EXCLUSIVE"},
        {PredicateCondition::kBetweenInclusive, "BETWEEN INCLUSIVE"},
        {PredicateCondition::kBetweenLowerExclusive, "BETWEEN LOWER EXCLUSIVE"},
        {PredicateCondition::kBetweenUpperExclusive, "BETWEEN UPPER EXCLUSIVE"},
        {PredicateCondition::kEquals, "="},
        {PredicateCondition::kGreaterThan, ">"},
        {PredicateCondition::kGreaterThanEquals, ">="},
        {PredicateCondition::kIn, "IN"},
        {PredicateCondition::kIsNotNull, "IS NOT NULL"},
        {PredicateCondition::kIsNull, "IS NULL"},
        {PredicateCondition::kLessThan, "<"},
        {PredicateCondition::kLessThanEquals, "<="},
        {PredicateCondition::kLike, "LIKE"},
        {PredicateCondition::kNotEquals, "!="},
        {PredicateCondition::kNotIn, "NOT IN"},
        {PredicateCondition::kNotLike, "NOT LIKE"},
    });

const boost::bimap<SetOperationMode, std::string> kSetOperationModeToString = MakeBimap<SetOperationMode, std::string>({
    {SetOperationMode::kAll, "All"},
    {SetOperationMode::kUnique, "Unique"},
});

const boost::bimap<SortMode, std::string> kSortModeToString = MakeBimap<SortMode, std::string>({
    {SortMode::kAscending, "Ascending"},
    {SortMode::kDescending, "Descending"},
});

}  // namespace skyrise
