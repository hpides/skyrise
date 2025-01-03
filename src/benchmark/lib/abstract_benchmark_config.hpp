#pragma once

#include <aws/core/Aws.h>

namespace skyrise {

class AbstractBenchmarkConfig {
 public:
  AbstractBenchmarkConfig(const size_t concurrent_instance_count, const size_t repetition_count,
                          const std::vector<std::function<void()>>& after_repetition_callbacks);

  virtual ~AbstractBenchmarkConfig() = default;

  Aws::String GetBenchmarkId() const;
  Aws::String GetBenchmarkTimestamp() const;
  size_t GetConcurrentInstanceCount() const;
  size_t GetRepetitionCount() const;

 protected:
  const Aws::String benchmark_id_;
  const Aws::String benchmark_timestamp_;
  const size_t concurrent_instance_count_;
  const size_t repetition_count_;
  const std::vector<std::function<void()>> after_repetition_callbacks_;
};

}  // namespace skyrise
