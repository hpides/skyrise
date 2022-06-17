/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <string>

#include "SQLParser.h"

namespace skyrise {

std::string CreateSqlParserErrorMessage(const std::string& sql, const hsql::SQLParserResult& result);

}  // namespace skyrise
