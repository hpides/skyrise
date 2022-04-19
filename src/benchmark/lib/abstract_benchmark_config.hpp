#pragma once

#include "utils/string.hpp"
#include "utils/time.hpp"

namespace skyrise {

class AbstractBenchmarkConfig {
 public:
  AbstractBenchmarkConfig(const size_t concurrent_invocation_count, const size_t repetition_count,
                          const std::vector<std::function<void()>>& after_repetition_callbacks)
      : concurrent_invocation_count_(concurrent_invocation_count),
        repetition_count_(repetition_count),
        after_repetition_callbacks_(after_repetition_callbacks.empty()
                                        ? std::vector<std::function<void()>>(repetition_count_, [] {})
                                        : after_repetition_callbacks),
        benchmark_id_(RandomString(8)),
        benchmark_timestamp_(GetFormattedTimestamp("%Y%m%dT%H%M%S")) {}

  virtual ~AbstractBenchmarkConfig() = default;

  const size_t concurrent_invocation_count_;
  const size_t repetition_count_;

  const std::vector<std::function<void()>> after_repetition_callbacks_;

  const Aws::String benchmark_id_;
  const Aws::String benchmark_timestamp_;
};

}  // namespace skyrise
