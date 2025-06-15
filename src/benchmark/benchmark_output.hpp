#pragma once

#include <string>

namespace skyrise {

class BenchmarkOutput {
 public:
  virtual ~BenchmarkOutput() = default;

  virtual void Write(const std::string& output) = 0;
};

}  // namespace skyrise 