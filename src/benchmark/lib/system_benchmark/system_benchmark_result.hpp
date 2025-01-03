#pragma once

#include <cstddef>

#include <aws/core/utils/json/JsonSerializer.h>

#include "abstract_benchmark_result.hpp"

namespace skyrise {
class SystemBenchmarkResult : public AbstractBenchmarkResult {
 public:
  explicit SystemBenchmarkResult(const Aws::Utils::Array<Aws::Utils::Json::JsonValue>& results);

  double GetDurationMs() const override;
  bool IsResultComplete() const override;

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> GetResults() const;

 protected:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results_;
};

}  // namespace skyrise
