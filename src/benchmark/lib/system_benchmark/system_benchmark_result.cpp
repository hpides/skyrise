#include "system_benchmark_result.hpp"

namespace skyrise {

SystemBenchmarkResult::SystemBenchmarkResult(const Aws::Utils::Array<Aws::Utils::Json::JsonValue>& results)
    : results_(results) {}

double SystemBenchmarkResult::GetDurationMs() const { return 0; }

bool SystemBenchmarkResult::IsResultComplete() const { return true; }

Aws::Utils::Array<Aws::Utils::Json::JsonValue> SystemBenchmarkResult::GetResults() const { return results_; }

}  // namespace skyrise
