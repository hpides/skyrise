/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstdlib>
#include <map>

namespace skyrise {

/**
 * Contains all supported TPCH queries. Use ordered map to have queries sorted by query id.
 * This allows for guaranteed execution order when iterating over the queries.
 */
extern const std::map<size_t, const char*> tpch_queries;

}  // namespace skyrise
