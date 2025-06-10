#include "abstract_benchmark_config.hpp"

#include "utils/string.hpp"
#include "utils/time.hpp"

namespace skyrise {

AbstractBenchmarkConfig::AbstractBenchmarkConfig(const size_t concurrent_instance_count, const size_t repetition_count,
                                                 const std::vector<std::function<void()>>& after_repetition_callbacks)
    : benchmark_id_(RandomString(8)),
      benchmark_timestamp_(GetFormattedTimestamp("%Y%m%dT%H%M%S")),
      concurrent_instance_count_(concurrent_instance_count),
      repetition_count_(repetition_count),
      after_repetition_callbacks_(after_repetition_callbacks.empty()
                                      ? std::vector<std::function<void()>>(repetition_count_, [] {})
                                      : after_repetition_callbacks) {}

Aws::String AbstractBenchmarkConfig::GetBenchmarkId() const { return benchmark_id_; }
Aws::String AbstractBenchmarkConfig::GetBenchmarkTimestamp() const { return benchmark_timestamp_; }
size_t AbstractBenchmarkConfig::GetConcurrentInstanceCount() const { return concurrent_instance_count_; }
size_t AbstractBenchmarkConfig::GetRepetitionCount() const { return repetition_count_; }

}  // namespace skyrise
