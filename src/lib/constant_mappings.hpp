/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <string>

#include <boost/bimap.hpp>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * In contrast to std::map or std::unordered_map, boost::bimap provides bidirectional access to its data:
 *  - Left values can be accessed using .left(...)
 *  - Right values can be accessed using .right(...)
 *
 *  For kDataTypeToString, for example, .left contains DataType enum values and .right contains the associated strings.
 */
extern const boost::bimap<AggregateFunction, std::string> kAggregateFunctionToString;
extern const boost::bimap<DataType, std::string> kDataTypeToString;
extern const boost::bimap<ExchangeMode, std::string> kExchangeModeToString;
extern const boost::bimap<JoinMode, std::string> kJoinModeToString;
extern const boost::bimap<PredicateCondition, std::string> kPredicateConditionToString;
extern const boost::bimap<SetOperationMode, std::string> kSetOperationModeToString;
extern const boost::bimap<SortMode, std::string> kSortModeToString;

}  // namespace skyrise
