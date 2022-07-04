#include "time.hpp"

#include <iomanip>
#include <sstream>

namespace skyrise {

std::string GetFormattedTimestamp(const std::string& format) {
  return GetFormattedTimestamp(std::time(nullptr), format);
}

std::string GetFormattedTimestamp(const std::time_t time_in_seconds, const std::string& format) {
  tm calendar_date{};
  localtime_r(&time_in_seconds, &calendar_date);

  std::stringstream timestamp;
  timestamp << std::put_time(&calendar_date, format.c_str());

  return timestamp.str();
}

}  // namespace skyrise
