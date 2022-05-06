/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "timer.hpp"

#include "utils/format_duration.hpp"

namespace skyrise {

Timer::Timer() { begin_ = std::chrono::high_resolution_clock::now(); }

std::chrono::nanoseconds Timer::Lap() {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto lap_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - begin_);
  begin_ = now;
  return lap_duration;
}

std::string Timer::LapFormatted() { return FormatDuration(Lap()); }

}  // namespace skyrise
