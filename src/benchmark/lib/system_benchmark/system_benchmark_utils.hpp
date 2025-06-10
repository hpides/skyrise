#pragma once

#include "types.hpp"

namespace skyrise {

CompilerName ParseCompilerName(const std::string& compiler_name_string);
QueryId ParseQueryId(const std::string& query_id_string);
ScaleFactor ParseScaleFactor(const std::string& scale_factor_string);

}  // namespace skyrise
