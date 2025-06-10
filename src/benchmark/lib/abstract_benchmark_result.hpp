#pragma once

namespace skyrise {

class AbstractBenchmarkResult {
 public:
  virtual ~AbstractBenchmarkResult() = default;

  virtual double GetDurationMs() const = 0;
  virtual bool IsResultComplete() const = 0;
};

}  // namespace skyrise
