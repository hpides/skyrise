#include "time.hpp"

#include <ctime>
#include <iomanip>
#include <sstream>

namespace skyrise {

std::string GetFormattedTimestamp(const std::string& format) {
  const time_t time = std::time(nullptr);
  tm calendar_date{};
  localtime_r(&time, &calendar_date);

  std::stringstream timestamp;
  timestamp << std::put_time(&calendar_date, format.c_str());

  return timestamp.str();
}

}  // namespace skyrise
