#include "system_benchmark_config.hpp"

namespace skyrise {

SystemBenchmarkConfig::SystemBenchmarkConfig(const CompilerName& compiler_name, const QueryId& query_id,
                                             const ScaleFactor& scale_factor, const size_t concurrent_instance_count,
                                             const size_t repetition_count,
                                             const std::vector<std::function<void()>>& after_repetition_callbacks)
    : AbstractBenchmarkConfig(concurrent_instance_count, repetition_count, after_repetition_callbacks),
      compiler_name_(compiler_name),
      query_id_(query_id),
      scale_factor_(scale_factor) {}

CompilerName SystemBenchmarkConfig::GetCompilerName() const { return compiler_name_; }
QueryId SystemBenchmarkConfig::GetQueryId() const { return query_id_; }
ScaleFactor SystemBenchmarkConfig::GetScaleFactor() const { return scale_factor_; }

}  // namespace skyrise
