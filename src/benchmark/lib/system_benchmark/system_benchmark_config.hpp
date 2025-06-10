#pragma once

#include "abstract_benchmark_config.hpp"
#include "types.hpp"

namespace skyrise {

class SystemBenchmarkConfig : public AbstractBenchmarkConfig {
 public:
  SystemBenchmarkConfig(const CompilerName& compiler_name, const QueryId& query_id, const ScaleFactor& scale_factor,
                        const size_t concurrent_instance_count, const size_t repetition_count,
                        const std::vector<std::function<void()>>& after_repetition_callbacks = {});

  CompilerName GetCompilerName() const;
  QueryId GetQueryId() const;
  ScaleFactor GetScaleFactor() const;

 protected:
  const CompilerName compiler_name_;
  const QueryId query_id_;
  const ScaleFactor scale_factor_;
};

}  // namespace skyrise
