#include "time.hpp"

#include <ctime>
#include <iomanip>
#include <sstream>

namespace skyrise {

std::string GetFormattedTimestamp(const std::string& format) {
  const auto time = std::time(nullptr);

  std::stringstream timestamp;
  timestamp << std::put_time(std::localtime(&time), format.c_str());

  return timestamp.str();
}

}  // namespace skyrise
