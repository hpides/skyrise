#pragma once

#include <string>

namespace skyrise {

// Produce a formatted timestamp similar to std::put_time (https://en.cppreference.com/w/cpp/io/manip/put_time)
std::string GetFormattedTimestamp(const std::string& format);

}  // namespace skyrise
