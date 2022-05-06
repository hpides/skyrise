/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <chrono>
#include <string>

namespace skyrise {

/**
 * Starts a std::chrono::high_resolution_clock base timer on construction and returns and resets measurement when
 * Lap() is called.
 */
class Timer final {
 public:
  Timer();

  /**
   * @return Time elapsed since construction or the last call to Lap(), whichever was later
   */
  std::chrono::nanoseconds Lap();

  /**
   * Calls Lap() and formats the result into a human-readable form
   */
  std::string LapFormatted();

 private:
  std::chrono::high_resolution_clock::time_point begin_;
};

}  // namespace skyrise
