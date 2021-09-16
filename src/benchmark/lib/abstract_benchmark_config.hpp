#pragma once

namespace skyrise {

class AbstractBenchmarkConfig {
 public:
  AbstractBenchmarkConfig(const size_t concurrent_invocation_count, const size_t repetition_count)
      : concurrent_invocation_count_(concurrent_invocation_count), repetition_count_(repetition_count) {}

  virtual ~AbstractBenchmarkConfig() = default;

  const size_t concurrent_invocation_count_;
  const size_t repetition_count_;
};

}  // namespace skyrise
