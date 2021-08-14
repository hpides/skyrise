/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <string>

#include <boost/bimap.hpp>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace skyrise {

extern const boost::bimap<AggregateFunction, std::string> kAggregateFunctionToString;
extern const boost::bimap<DataType, std::string> kDataTypeToString;
extern const boost::bimap<JoinMode, std::string> kJoinModeToString;
extern const boost::bimap<PredicateCondition, std::string> kPredicateConditionToString;
extern const boost::bimap<SetOperationMode, std::string> kSetOperationModeToString;
extern const boost::bimap<SortMode, std::string> kSortModeToString;

}  // namespace skyrise
